mod constants;

use constants::{
    COLORS, CONTAINER_SIZE, CONTAINER_TYPE, TYPE_SYLLABLE1, TYPE_SYLLABLE2, TYPE_SYLLABLE3,
};
use populate_builtins::{
    JoinExt, JoinTuple2Ext, JoinTuple3Ext, Pick, inc, pick_one, range, tpch_bounded_int,
};
use populate_core::Distribution;

/// (p_partkey, p_name, p_type, p_size, p_container, p_mfgr_num, p_brand_num)
pub fn parts() -> impl Distribution<(i64, String, String, i64, String, i32, i32)> {
    (
        inc(1i64),                            // p_partkey
        Pick::<_, 5>::new(&COLORS).join(" "), // p_name: 5 random colors joined
        (
            pick_one(&TYPE_SYLLABLE3),
            pick_one(&TYPE_SYLLABLE2),
            pick_one(&TYPE_SYLLABLE1),
        )
            .join3(" "), // p_type: SYLLABLE3 + " " + SYLLABLE2 + " " + SYLLABLE1
        tpch_bounded_int(1193163244, 1, 50),  // p_size: 1-50
        (pick_one(&CONTAINER_SIZE), pick_one(&CONTAINER_TYPE)).join2(" "), // p_container: SIZE + " " + TYPE
        range(1i32, 6),                                                    // p_mfgr: 1-5
        range(1i32, 6),                                                    // p_brand: 1-5
    )
}

/// Check if a name is valid (5 space-separated colors)
pub fn is_valid_name(name: &str) -> bool {
    let words: Vec<&str> = name.split(' ').collect();
    words.len() == 5 && words.iter().all(|w| COLORS.contains(w))
}

/// Check if a type is valid (SYLLABLE3 SYLLABLE2 SYLLABLE1)
pub fn is_valid_type(typ: &str) -> bool {
    let parts: Vec<&str> = typ.split(' ').collect();
    parts.len() == 3
        && TYPE_SYLLABLE3.contains(&parts[0])
        && TYPE_SYLLABLE2.contains(&parts[1])
        && TYPE_SYLLABLE1.contains(&parts[2])
}

/// Check if a container is valid (SIZE TYPE)
pub fn is_valid_container(container: &str) -> bool {
    let parts: Vec<&str> = container.split(' ').collect();
    parts.len() == 2 && CONTAINER_SIZE.contains(&parts[0]) && CONTAINER_TYPE.contains(&parts[1])
}
