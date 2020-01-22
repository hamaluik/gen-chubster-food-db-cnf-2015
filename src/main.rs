use calamine::{Reader, Xlsx};
use rusqlite::{params};
use std::fs::File;
use std::io::Cursor;

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
    let mem_con = rusqlite::Connection::open_in_memory().expect("can open sqlite in memory");
    mem_con
        .execute_batch(
            r#"
    begin;
    create table nutrients(
        food_id integer not null,
        nutrient_id integer not null,
        nutrient_value real not null
    );
    create index nutrients_food_id on nutrients(food_id);
    commit;
    "#,
        )
        .expect("can initialize tables");

    // load up our foods
    let mut food_ids: Vec<(i64, String)> = Vec::with_capacity(5000);
    println!("loading food names & ids...");
    {
        let foods_file = File::open("cnf/FOOD NAME.xlsx").expect("can open FOOD NAME.xlsx");
        let foods_file =
            unsafe { memmap::Mmap::map(&foods_file).expect("can memmap FOOD NAME.xlsx") };
        let mut foods = Xlsx::new(Cursor::new(&foods_file[..])).expect("can load FOOD NAME.xlsx");
        let worksheet_name = foods.sheet_names()[0].to_owned();
        let range = foods
            .worksheet_range(&worksheet_name)
            .expect("valid sheet")
            .expect("can open FOOD NAME worksheet");

        for row in range.rows().skip(1) {
            let id: f64 = row[0].get_float().expect("float food id");
            let id: i64 = id as i64;
            let description = row[4].get_string().expect("string food description");
            food_ids.push((id, description.to_owned()));
        }
    }

    // load up our nutrients
    println!("loading nutrient values...");
    {
        let nutrients_file =
            File::open("cnf/NUTRIENT AMOUNT.xlsx").expect("can open NUTRIENT AMOUNT.xlsx");
        let nutrients_file =
            unsafe { memmap::Mmap::map(&nutrients_file).expect("can memmap NUTRIENT AMOUNT.xlsx") };
        let mut nutrients =
            Xlsx::new(Cursor::new(&nutrients_file[..])).expect("can load NUTRIENT AMOUNT.xlsx");
        let worksheet_name = nutrients.sheet_names()[0].to_owned();
        let range = nutrients
            .worksheet_range(&worksheet_name)
            .expect("valid sheet")
            .expect("can open NUTRIENT AMOUNT worksheet");

        let mut stmt = mem_con
            .prepare_cached(
                "insert into nutrients(food_id, nutrient_id, nutrient_value) values(?, ?, ?)",
            )
            .expect("valid insert query");
        for row in range.rows().skip(1) {
            let food_id: f64 = row[0].get_float().expect("float food id");
            let food_id: i64 = food_id as i64;

            let nutrient_id: f64 = row[1].get_float().expect("float nutrient id");
            let nutrient_id: i64 = nutrient_id as i64;

            let nutrient_value: f64 = row[2].get_float().expect("float nutrient value");
            stmt.execute(params![food_id, nutrient_id, nutrient_value])
                .expect("can insert nutrient");
        }
    }

    // open the output database
    println!("opening output database...");
    if std::path::Path::new("cnf.db").exists() {
        std::fs::remove_file("cnf.db").expect("can delete existing cnf.db file");
    }
    let db = rusqlite::Connection::open("cnf.db").expect("can create cnf.db");
    db.execute_batch(
        r#"
        begin;
        PRAGMA foreign_keys = ON;
        create table foods(
            id integer not null primary key autoincrement,
            name text not null,
            energy real default null,
            fat_total real default null,
            fat_saturated real default null,
            fat_trans real default null,
            fat_polyunsaturated real default null,
            fat_monounsaturated real default null,
            cholesterol real default null,
            sodium real default null,
            carbohydrates real default null,
            fiber real default null,
            sugars real default null,
            protein real default null,
            calcium real default null,
            potassium real default null,
            iron real default null,
            alcohol real default null,
            caffeine real default null
        );
        create index food_name on foods(name);
        create table measurements(
            id integer not null primary key autoincrement,
            description text not null
        );
        create table conversions(
            food_id integer not null,
            measurement_id integer not null,
            conversion_factor real not null,
            unique(food_id, measurement_id),
            foreign key(food_id) references foods(id),
            foreign key(measurement_id) references measurements(id)
        );
        create index conversions_food_id on conversions(food_id);
        commit;
    "#,
    )
    .expect("can initialize chubster tables");

    // load up our measurements
    println!("loading measurement values...");
    {
        let measurements_file =
            File::open("cnf/MEASURE NAME.xlsx").expect("can open MEASURE NAME.xlsx");
        let measurements_file =
            unsafe { memmap::Mmap::map(&measurements_file).expect("can memmap MEASURE NAME.xlsx") };
        let mut measurements =
            Xlsx::new(Cursor::new(&measurements_file[..])).expect("can load MEASURE NAME.xlsx");
        let worksheet_name = measurements.sheet_names()[0].to_owned();
        let range = measurements
            .worksheet_range(&worksheet_name)
            .expect("valid sheet")
            .expect("can open MEASURE NAME worksheet");

        let mut stmt = db
            .prepare_cached(
                "insert into measurements(id, description) values(?, ?)",
            )
            .expect("valid insert query");
        for row in range.rows().skip(1) {
            let measurement_id: f64 = row[0].get_float().expect("float measurement id");
            let measurement_id: i64 = measurement_id as i64;

            let description: &str = row[1].get_string().expect("string measurement value");
            stmt.execute(params![measurement_id, description])
                .expect("can insert measurement");
        }
    }

    // start going through the foods
    println!("processing foods...");
    {
        let pb = indicatif::ProgressBar::new(food_ids.len() as u64);
        pb.set_style(indicatif::ProgressStyle::default_bar()
            .template("{pos:>7}/{len:7} {wide_bar} [{elapsed_precise}] ({eta_precise})"));
        let mut nutrient_stmt = mem_con
            .prepare_cached("select nutrient_id, nutrient_value from nutrients where food_id=?")
            .expect("valid select query");
        let mut insert_food_stmt = db
            .prepare_cached("insert into foods(id, name, energy, fat_total, fat_saturated, fat_trans, fat_polyunsaturated, fat_monounsaturated, cholesterol, sodium, carbohydrates, fiber, sugars, protein, calcium, potassium, iron, alcohol, caffeine) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .expect("valid insert query");
        for (food_id, food_description) in food_ids.iter() {
            // get all the nutrients
            let nutrients = nutrient_stmt
                .query_map(params![food_id], |row| {
                    let nutient_id: i64 = row.get(0)?;
                    let nutrient_value: f64 = row.get(1)?;
                    Ok((nutient_id, nutrient_value))
                })
                .expect("can execute select query");

            let mut energy: Option<f64> = None;
            let mut fat_total: Option<f64> = None;
            let mut fat_saturated: Option<f64> = None;
            let mut fat_poly: Option<f64> = None;
            let mut fat_mono: Option<f64> = None;
            let mut fat_trans: Option<f64> = None;
            let mut cholesterol: Option<f64> = None;
            let mut sodium: Option<f64> = None;
            let mut carbs: Option<f64> = None;
            let mut fiber: Option<f64> = None;
            let mut sugars: Option<f64> = None;
            let mut protein: Option<f64> = None;
            let mut calcium: Option<f64> = None;
            let mut potassium: Option<f64> = None;
            let mut iron: Option<f64> = None;
            let mut alcohol: Option<f64> = None;
            let mut caffeine: Option<f64> = None;
            for nutrient in nutrients {
                let nutrient = nutrient.expect("valid nutrient result");
                match nutrient.0 {
                    ENERGY_ID => energy = Some(nutrient.1),
                    FAT_TOTAL_ID => fat_total = Some(nutrient.1),
                    FAT_SATURATED_ID => fat_saturated = Some(nutrient.1),
                    FAT_POLY_ID => fat_poly = Some(nutrient.1),
                    FAT_MONO_ID => fat_mono = Some(nutrient.1),
                    FAT_TRANS_ID => fat_trans = Some(nutrient.1),
                    CHOLESTEROL_ID => cholesterol = Some(nutrient.1),
                    SODIUM_ID => sodium = Some(nutrient.1),
                    CARBS_ID => carbs = Some(nutrient.1),
                    FIBER_ID => fiber = Some(nutrient.1),
                    SUGARS_ID => sugars = Some(nutrient.1),
                    PROTEIN_ID => protein = Some(nutrient.1),
                    CALCIUM_ID => calcium = Some(nutrient.1),
                    POTASSIUM_ID => potassium = Some(nutrient.1),
                    IRON_ID => iron = Some(nutrient.1),
                    ALCOHOL_ID => alcohol = Some(nutrient.1),
                    CAFFEINE_ID => caffeine = Some(nutrient.1),
                    _ => {} // skip nutrients we don't care about
                }
            }

            insert_food_stmt.execute(params![food_id, food_description, energy, fat_total, fat_saturated, fat_trans, fat_poly, fat_mono, cholesterol, sodium, carbs, fiber, sugars, protein, calcium, potassium, iron, alcohol, caffeine]).expect("can insert food");

            pb.inc(1);
        }
        pb.finish_with_message("done");
    }

    // load up our conversions
    println!("loading conversion values...");
    {
        let conversions_file =
            File::open("cnf/CONVERSION FACTOR.xlsx").expect("can open CONVERSION FACTOR.xlsx");
        let conversions_file =
            unsafe { memmap::Mmap::map(&conversions_file).expect("can memmap CONVERSION FACTOR.xlsx") };
        let mut conversions =
            Xlsx::new(Cursor::new(&conversions_file[..])).expect("can load CONVERSION FACTOR.xlsx");
        let worksheet_name = conversions.sheet_names()[0].to_owned();
        let range = conversions
            .worksheet_range(&worksheet_name)
            .expect("valid sheet")
            .expect("can open CONVERSION FACTOR worksheet");

        let mut stmt = db
            .prepare_cached(
                "insert into conversions(food_id, measurement_id, conversion_factor) values(?, ?, ?)",
            )
            .expect("valid insert query");
        for row in range.rows().skip(1) {
            let food_id: f64 = row[0].get_float().expect("float food_id");
            let food_id: i64 = food_id as i64;

            let measurement_id: f64 = row[1].get_float().expect("float measurement_id");
            let measurement_id: i64 = measurement_id as i64;

            let conversion_factor: f64 = row[2].get_float().expect("float conversion_factor");

            if let Err(e) = stmt.execute(params![food_id, measurement_id, conversion_factor]) {
                let food_count: i64 = db.query_row("select count(*) from foods where id=?", params![food_id], |row| row.get(0)).expect("can query food id count");
                let measurement_count: i64 = db.query_row("select count(*) from measurements where id=?", params![measurement_id], |row| row.get(0)).expect("can query measurement id count");

                eprintln!("WARNING: failed to insert conversion for food_id: {} (found {} matching rows), measurement_id: {} (found {} matching rows)\n{:?}", food_id, food_count, measurement_id, measurement_count, e);
            }
        }
    }

    // NOTE: all nutrition values in the foods table are per 100g
    // To get all common measurements and conversions for a given food id, you can
    // join on the measurements and conversions tables. For example:
    //
    // ```sql
    // select description, conversion_factor from conversions inner join measurements on measurements.id=conversions.measurement_id where food_id=?
    // ```

    drop(mem_con);
    drop(db);
    println!("database saved to `cnf.db`!");
}
