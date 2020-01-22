use std::fs::File;
use calamine::{Reader, Xlsx};
use std::io::Cursor;
use rusqlite::{NO_PARAMS, params};

const ENERGY_ID: i64 = 208;
const FAT_TOTAL_ID: i64 = 204;
const FAT_SATURATED_ID: i64 = 606;
const FAT_POLY_ID: i64 = 646;
const FAT_MONO_ID: i64 = 645;
const FAT_TRANS_ID: i64 = 605;
const CHOLESTEROL_ID: i64 = 601;
const SODIUM_ID: i64 = 307;
const CARBS_ID: i64 = 205;
const FIBER_ID: i64 = 291;
const SUGARS_ID: i64 = 269;
const PROTEIN_ID: i64 = 203;
const CALCIUM_ID: i64 = 301;
const POTASSIUM_ID: i64 = 306;
const IRON_ID: i64 = 303;
const ALCOHOL_ID: i64 = 221;
const CAFFEINE_ID: i64 = 262;

fn main() {
    // start our database that we can work off of
    println!("opening in-mem DB representation..");
    let conn = rusqlite::Connection::open_in_memory().expect("can open sqlite in memory");
    conn.execute_batch(r#"
    begin;
    create table foods(
        id integer not null primary key,
        description text not null
    );
    create table nutrients(
        food_id integer not null,
        nutrient_id integer not null,
        nutrient_value real not null,
        foreign key(food_id) references foods(id)
    );
    commit;
    "#).expect("can initialize tables");

    // load up our foods
    println!("loading food names & ids...");
    {
        let foods_file = File::open("cnf/FOOD NAME.xlsx").expect("can open FOOD NAME.xlsx");
        let foods_file = unsafe { memmap::Mmap::map(&foods_file).expect("can memmap FOOD NAME.xlsx") };
        let mut foods = Xlsx::new(Cursor::new(&foods_file[..])).expect("can load FOOD NAME.xlsx");
        let worksheet_name = foods.sheet_names()[0].to_owned();
        let range = foods.worksheet_range(&worksheet_name).expect("valid sheet").expect("can open FOOD NAME worksheet");

        let mut stmt = conn.prepare_cached("insert into foods(id, description) values(?, ?)").expect("valid insert query");
        for row in range.rows().skip(1) {
            let id: f64 = row[0].get_float().expect("float food id");
            let id: i64 = id as i64;
            let description = row[4].get_string().expect("string food description");
            stmt.execute(params![id, description]).expect("can insert food");
        }
    }

    // load up our nutrients
    println!("loading nutrient values...");
    {
        let nutrients_file = File::open("cnf/NUTRIENT AMOUNT.xlsx").expect("can open NUTRIENT AMOUNT.xlsx");
        let nutrients_file = unsafe { memmap::Mmap::map(&nutrients_file).expect("can memmap NUTRIENT AMOUNT.xlsx") };
        let mut nutrients = Xlsx::new(Cursor::new(&nutrients_file[..])).expect("can load NUTRIENT AMOUNT.xlsx");
        let worksheet_name = nutrients.sheet_names()[0].to_owned();
        let range = nutrients.worksheet_range(&worksheet_name).expect("valid sheet").expect("can open NUTRIENT AMOUNT worksheet");

        let mut stmt = conn.prepare_cached("insert into nutrients(food_id, nutrient_id, nutrient_value) values(?, ?, ?)").expect("valid insert query");
        for row in range.rows().skip(1) {
            let food_id: f64 = row[0].get_float().expect("float food id");
            let food_id: i64 = food_id as i64;

            let nutrient_id: f64 = row[1].get_float().expect("float nutrient id");
            let nutrient_id: i64 = nutrient_id as i64;
            
            let nutrient_value: f64 = row[2].get_float().expect("float nutrient value");
            stmt.execute(params![food_id, nutrient_id, nutrient_value]).expect("can insert nutrient");
        }
    }
}
