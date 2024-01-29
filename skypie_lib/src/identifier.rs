pub trait Identifier<T>
{
    fn get_id(self: &Self) -> T;
}