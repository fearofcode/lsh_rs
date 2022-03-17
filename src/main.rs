use std::cmp::Reverse;
use std::collections::hash_map::DefaultHasher;
use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};

use rand::prelude::ThreadRng;
use rand::Rng;
use rayon::prelude::*;

const HASH_COUNT: usize = 10;
const BAND_SIZE: usize = 2;
const SHINGLE_SIZE: usize = 3;

// constants for synthetic data
const DOCUMENT_LEN: usize = 100;
const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn random_char(mut rng: &mut ThreadRng) -> char {
    let idx = rng.gen_range(0..CHARSET.len());
    CHARSET[idx] as char
}

fn min_hash(document: &str) -> Vec<u64> {
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

    hashes
}

fn generate_random_string(mut rng: &mut ThreadRng, random_string: &String) -> String {
    let random_op: i32 = rng.gen_range(0..3);

    let change_size = 5;
    let op_start = rng.gen_range(change_size..(PASSWORD_LEN - change_size - 1));
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

    let altered_string = generate_random_string(&mut rng, &random_string);

    println!("{:?}: {:?}", random_string, min_hash(&random_string));
    println!("{:?}: {:?}", altered_string, min_hash(&altered_string));

    let documents = vec![random_string, altered_string];

    let min_hashes: Vec<Vec<u64>> = documents
        .par_iter()
        .map(|document| min_hash(&document))
        .collect();
}
