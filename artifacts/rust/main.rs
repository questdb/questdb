use postgres::{Client, NoTls};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("postgresql://admin:quest@localhost:5432/qdb", NoTls)?;
    let bar = 1;
    let baz = 2;
    let roe = 3;
    let mut txn = client.transaction()?;
    let statement = txn.prepare("insert into xyz values ($1,$2,$3)")?;

    for n in 0..1_000_0 {
        txn.execute(&statement, &[&bar, &baz, &roe])?;
    }
    txn.commit()?;
    println!("import finished");
    Ok(())
}
