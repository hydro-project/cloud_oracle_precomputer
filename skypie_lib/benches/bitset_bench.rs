#![feature(test)]

extern crate hibitset;
extern crate test;

mod bitset {
    use std::ops::{BitAndAssign, BitAnd};
    use rand::prelude::*;

    use hibitset::{BitSet, BitSetLike};
    use test;

    #[bench]
    fn add(b: &mut test::Bencher) {
        let mut bitset = BitSet::with_capacity(1_000_000);
        let mut range = (0..1_000_000).cycle();
        b.iter(|| range.next().map(|i| bitset.add(i)))
    }

    #[bench]
    fn remove_set(b: &mut test::Bencher) {
        let mut bitset = BitSet::with_capacity(1_000_000);
        let mut range = (0..1_000_000).cycle();
        for i in 0..1_000_000 {
            bitset.add(i);
        }
        b.iter(|| range.next().map(|i| bitset.remove(i)))
    }

    #[bench]
    fn remove_clear(b: &mut test::Bencher) {
        let mut bitset = BitSet::with_capacity(1_000_000);
        let mut range = (0..1_000_000).cycle();
        b.iter(|| range.next().map(|i| bitset.remove(i)))
    }

    #[bench]
    fn contains(b: &mut test::Bencher) {
        let mut bitset = BitSet::with_capacity(1_000_000);
        let mut range = (0..1_000_000).cycle();
        for i in 0..500_000 {
            // events are set, odds are to keep the branch
            // prediction from getting to aggressive
            bitset.add(i * 2);
        }
        b.iter(|| range.next().map(|i| bitset.contains(i)))
    }

    #[bench]
    fn contains_set_naive(b: &mut test::Bencher) {
        let mut bitset = BitSet::with_capacity(1_000_000);
        let mut range = (0..1_000_000).cycle();
        for i in 0..500_000 {
            // events are set, odds are to keep the branch
            // prediction from getting to aggressive
            bitset.add(i * 2);
        }
        //let rhs_bitset = bitset.clone();
        let mut rhs_bitset = BitSet::with_capacity(1_000_000);
        for i in 0..1_000_000 {
            rhs_bitset.add(i);
        }
        b.iter(|| range.next().map(|i| {
            if bitset.contains(i) && rhs_bitset.contains(i) {
                bitset.add(i);
            }
        }))
    }
    
    #[bench]
    fn contains_set_fast(b: &mut test::Bencher) {
        let mut bitset = BitSet::with_capacity(1_000_000);

        let mut range = (0..1_000_000).cycle();

        for i in 0..500_000 {
            // events are set, odds are to keep the branch
            // prediction from getting to aggressive
            bitset.add(i * 2);
        }
        //let rhs_bitset = bitset.clone();
        let mut rhs_bitset = BitSet::with_capacity(1_000_000);
        for i in 0..1_000_000 {
            rhs_bitset.add(i);
        }
        b.iter(|| range.next().map(|i| {
            if !rhs_bitset.contains(i) {
                bitset.remove(i);
            }
        }))
    }

    #[bench]
    fn contains_set_fast_random(b: &mut test::Bencher) {
        let mut bitset = BitSet::with_capacity(1_000_000);

        let mut rng = thread_rng();
        let mut rand_range = (0..1_000_000).choose_multiple(&mut rng, 10000);
        rand_range.sort();
        let mut range = rand_range.into_iter().cycle();

        for i in 0..500_000 {
            // events are set, odds are to keep the branch
            // prediction from getting to aggressive
            bitset.add(i * 2);
        }
        //let rhs_bitset = bitset.clone();
        let mut rhs_bitset = BitSet::with_capacity(1_000_000);
        for i in 0..1_000_000 {
            rhs_bitset.add(i);
        }

        b.iter(|| range.next().map(|i| {
            if !rhs_bitset.contains(i) {
                bitset.remove(i);
            }
        }))
    }
    
    #[bench]
    fn bitand_assign(b: &mut test::Bencher) {
        let mut bitset = BitSet::with_capacity(1_000_000);
        let mut range = (0..1_000).cycle();
        for i in 0..500_000 {
            // events are set, odds are to keep the branch
            // prediction from getting to aggressive
            bitset.add(i * 2);
        }
        let rhs_bitset = bitset.clone();
        b.iter(|| range.next().map(|_| bitset.bitand_assign(&rhs_bitset)))
    }

    macro_rules! generate_bitand_assign_random {
        ($name:ident, $size:expr, $num_set_bits:expr, $bitsets:expr) => {
            #[bench]
            fn $name(b: &mut test::Bencher) {
                let mut rng = thread_rng();
                let mut bitset = BitSet::from_iter((0..$size).choose_multiple(&mut rng, $num_set_bits));
                let mut bitsets: Vec<BitSet> = Vec::with_capacity($bitsets);
                for _ in 0..$bitsets {
                    bitsets.push(BitSet::from_iter((0..$size).choose_multiple(&mut rng, $num_set_bits)));
                }
                //let mut range = (0..1_000).cycle();

                b.iter(|| bitsets.iter().for_each(|rhs_bitset| bitset.bitand_assign(rhs_bitset)))
            }
        };
    }

    generate_bitand_assign_random!(bitand_assign_random_max_1, 16_777_216 , 1, 1);
    generate_bitand_assign_random!(bitand_assign_random_max_100k, 16_777_216 , 100_000, 1);
    generate_bitand_assign_random!(bitand_assign_random_max_1m, 16_777_216 , 1_000_000, 1);
    generate_bitand_assign_random!(bitand_assign_random_max_10m, 16_777_216 , 10_000_000, 1);
    generate_bitand_assign_random!(bitand_assign_random_max_10m_2, 16_777_216 , 10_000_000, 2);
    generate_bitand_assign_random!(bitand_assign_random_max_10m_10, 16_777_216 , 10_000_000, 10);
    generate_bitand_assign_random!(bitand_assign_random_max_10m_20, 16_777_216 , 10_000_000, 20);

    use rayon::prelude::*;
    #[bench]
    fn parallel_reduce(b: &mut test::Bencher) {
        let no_bitsets = 3;
        let size = 1000;
        let num_set_bits = 100;
        
        let mut rng = thread_rng();

        let mut bitsets: Vec<BitSet> = Vec::with_capacity(no_bitsets);
        for _ in 0..no_bitsets {
            bitsets.push(BitSet::from_iter((0..size).choose_multiple(&mut rng, num_set_bits)));
        }

        b.iter(|| {
            let mut cloned = bitsets.clone();
            let res = cloned.par_iter_mut().reduce_with(|a, b| {a.bitand_assign(b); a}).unwrap();
            res.is_empty()
        })
    }
}

mod atomic_bitset {
    use hibitset::AtomicBitSet;
    use test;

    #[bench]
    fn add(b: &mut test::Bencher) {
        let mut bitset = AtomicBitSet::new();
        let mut range = (0..1_000_000).cycle();
        b.iter(|| range.next().map(|i| bitset.add(i)))
    }

    #[bench]
    fn add_atomic(b: &mut test::Bencher) {
        let bitset = AtomicBitSet::new();
        let mut range = (0..1_000_000).cycle();
        b.iter(|| range.next().map(|i| bitset.add_atomic(i)))
    }

    #[bench]
    fn remove_set(b: &mut test::Bencher) {
        let mut bitset = AtomicBitSet::new();
        let mut range = (0..1_000_000).cycle();
        for i in 0..1_000_000 {
            bitset.add(i);
        }
        b.iter(|| range.next().map(|i| bitset.remove(i)))
    }

    #[bench]
    fn remove_clear(b: &mut test::Bencher) {
        let mut bitset = AtomicBitSet::new();
        let mut range = (0..1_000_000).cycle();
        b.iter(|| range.next().map(|i| bitset.remove(i)))
    }

    #[bench]
    fn contains(b: &mut test::Bencher) {
        let mut bitset = AtomicBitSet::new();
        let mut range = (0..1_000_000).cycle();
        for i in 0..500_000 {
            // events are set, odds are to keep the branch
            // prediction from getting to aggressive
            bitset.add(i * 2);
        }
        b.iter(|| range.next().map(|i| bitset.contains(i)))
    }
}