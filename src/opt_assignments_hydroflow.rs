pub(crate) fn opt_assignments_hydroflow() {
    
    /* let mut flow = hydroflow_syntax! {
        // How to move ownerhship of the reader into the flow, so that we can directly iterate over it?
        source_in = source_iter(iter);
        //source_in = source_iter(CSVWrapper::new(path));
        source_in->map(|record|{convert_record(record.unwrap())})    
        ->map(|x|{sum_object_size(x, &size_map)})
        ->fold::<'tick>(0,|mut count, x| {
            count = write_record(count, x, &mut wtr);
            count
        })
        ->for_each(|x| println!("x: {:?}", x));
    };

    flow.run_available(); */
}