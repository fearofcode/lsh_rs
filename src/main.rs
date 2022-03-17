use std::cmp::Reverse;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use rand::prelude::ThreadRng;
use rand::Rng;
use rand::seq::SliceRandom;
use rayon::prelude::*;

const HASH_COUNT: usize = 10;
const BAND_SIZE: usize = 2;
const SHINGLE_SIZE: usize = 3;

// constants for synthetic data
const ORIGINAL_DOCUMENT_COUNT: usize = 1000;
const PER_DOCUMENT_MUTATION_COUNT: usize = 9; // 1000 + 9*1000 = 10000 total documents
const DOCUMENT_LEN: usize = 100;
const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn random_char(rng: &mut ThreadRng) -> char {
    let idx = rng.gen_range(0..CHARSET.len());
    CHARSET[idx] as char
}

fn chunked_min_hash(document: &str) -> Vec<(usize, u64)> {
    // single hash function. for justification, see https://robertheaton.com/2014/05/02/jaccard-similarity-and-minhash-for-winners/
    // and http://web.eecs.utk.edu/~jplank/plank/classes/cs494/494/notes/Min-Hash/index.html
    let shingle_count = document.len() - SHINGLE_SIZE;
    let mut heap = BinaryHeap::with_capacity(shingle_count);

    let mut hashes = vec![];
    for idx in 0..shingle_count {
        let shingle = &document[idx..idx + SHINGLE_SIZE];
        let mut hasher = DefaultHasher::new();
        shingle.hash(&mut hasher);
        let shingle_hash = hasher.finish();
        heap.push(Reverse(shingle_hash));
    }

    for _ in 0..HASH_COUNT {
        hashes.push(heap.pop().unwrap().0);
    }

    hashes.chunks(BAND_SIZE).map(|chunk| {
        let mut hasher = DefaultHasher::new();
        chunk.hash(&mut hasher);
        hasher.finish()
    }).enumerate().collect()
}

fn string_shingles(document: &str) -> HashSet<u64> {
    let shingle_count = document.len() - SHINGLE_SIZE;
    let mut shingles = HashSet::new();
    for idx in 0..shingle_count {
        let shingle = &document[idx..idx + SHINGLE_SIZE];
        let mut hasher = DefaultHasher::new();
        shingle.hash(&mut hasher);
        let shingle_hash = hasher.finish();
        shingles.insert(shingle_hash);
    }
    shingles
}

fn jaccard_similarity(a: &HashSet<u64>, b: &HashSet<u64>) -> f32 {
    let intersection_cardinality = a.intersection(b).count();
    (intersection_cardinality as f32) / ((a.len() + b.len() - intersection_cardinality) as f32)
}

fn nearest_neighbors(query: &str, n: usize, matches: HashSet<usize>, documents: Vec<String>) -> Vec<(usize, f32)> {
    let query_shingles = string_shingles(query);
    let mut similar_matches: Vec<(usize, f32)> = matches
        .par_iter()
        .map(|m| {
            let document = &documents[*m];
            let match_shingles = string_shingles(document);
            let similarity = jaccard_similarity(&query_shingles, &match_shingles);
            (*m, similarity)
        }).collect();
    similar_matches.sort_unstable_by(|a, b| b.partial_cmp(a).unwrap());
    similar_matches.resize(n, (0, 0.0));
    similar_matches
}

fn generate_random_string(mut rng: &mut ThreadRng, random_string: &String) -> String {
    let random_op: i32 = rng.gen_range(0..3);

    let change_size = 5;
    let op_start = rng.gen_range(change_size..(random_string.len() - change_size - 1));
    let op_end = op_start + change_size;
    let mut altered_string = random_string.clone();
    if random_op == 0 {
        // insert
        for _ in op_start..=op_end {
            altered_string.insert(op_start, random_char(&mut rng));
        }
    } else if random_op == 1 {
        // delete
        for _ in op_start..=op_end {
            altered_string.remove(op_start);
        }
    } else {
        // delete then insert
        for _ in op_start..=op_end {
            altered_string.remove(op_start);
        }
        for _ in op_start..=op_end {
            altered_string.insert(op_start, random_char(&mut rng));
        }
    }
    altered_string
}

fn main() {
    assert_eq!(HASH_COUNT % BAND_SIZE, 0);
    let mut rng = rand::thread_rng();

    let random_string: String = (0..DOCUMENT_LEN).map(|_| random_char(&mut rng)).collect();

    let mut documents = vec![random_string.clone()];
    documents.shuffle(&mut rng);

    let mut altered_string = random_string.clone();
    for _ in 0..PER_DOCUMENT_MUTATION_COUNT {
        altered_string = generate_random_string(&mut rng, &altered_string);
        documents.push(altered_string.clone());
    }
    let mut buckets: Vec<HashMap<u64, Vec<usize>>> = vec![];

    let bucket_count = HASH_COUNT / BAND_SIZE;
    for _ in 0..bucket_count {
        buckets.push(HashMap::new());
    }

    let chunked_min_hashes: Vec<Vec<(usize, u64)>> = documents
        .par_iter()
        .map(|document| chunked_min_hash(&document))
        .collect();

    for (document_index, chunked_min_hash) in chunked_min_hashes.iter().enumerate() {
        for (bucket_index, min_hash) in chunked_min_hash.iter() {
            let bucket = &mut buckets[*bucket_index];
            bucket.entry(min_hash.clone()).or_insert(vec![]).push(document_index.clone());
        }
    }

    let mut matches: HashSet<usize> = HashSet::new();
    let query_signature = chunked_min_hash(&documents[0]);
    for (bucket_index, min_hash) in query_signature.iter() {
        let bucket = &mut buckets[*bucket_index];
        if bucket.contains_key(min_hash) {
            matches.extend(&bucket[min_hash]);
        }
    }
}
