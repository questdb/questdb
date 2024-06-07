use std::io::{Read, Seek, Write};
use std::{mem, slice};
use anyhow::{anyhow, Result};

pub fn read_str(reader: &mut impl Read) -> Result<String> {
    unsafe {
        let mut name_len: i32 = 0;
        let buffer: &mut [u8] = std::slice::from_raw_parts_mut(
            &mut name_len as *mut i32 as *mut u8,
            4,
        );

        reader.read_exact(buffer)?;
        if name_len > 0 {
            let name_size_chars = name_len as usize;
            let mut name_arr: Box<[u16]> = vec![0u16; name_size_chars].into_boxed_slice();
            let ptr = Box::into_raw(name_arr);
            let str_buffer: &mut [u8] = std::slice::from_raw_parts_mut(
                ptr as *mut u8, name_size_chars * 2,
            );
            reader.read_exact(str_buffer)?;
            name_arr = Box::from_raw(ptr);
            let col_name: String = String::from_utf16(&*name_arr)?;
            return Ok(col_name);
        }
        return Err(anyhow!("invalid column length {}", name_len));
    };
}

pub fn read<T>(mut value: T, reader: &mut impl Read) -> Result<T> {
    unsafe {
        let buffer: &mut [u8] = std::slice::from_raw_parts_mut(
            &mut value as *mut T as *mut u8,
            mem::size_of::<T>(),
        );
        reader.read_exact(buffer)?;
        return Ok(value);
    };
}


pub unsafe fn transmute_slice<T>(slice: &[u8]) -> &[T] {
    let sizeof_t = mem::size_of::<T>();
    assert!(
        slice.len() % sizeof_t == 0,
        "slice.len() {} % sizeof_t {} != 0",
        slice.len(),
        sizeof_t
    );
    slice::from_raw_parts(slice.as_ptr() as *const T, slice.len() / sizeof_t)
}

pub fn read_vec<T>(reader: &mut impl Read, count: usize) -> Result<Vec<T>> {
    let mut vec: Vec<T> = Vec::with_capacity(count);
    unsafe {
        vec.set_len(count);
        let buffer: &mut [u8] = std::slice::from_raw_parts_mut(
            vec.as_mut_ptr() as *mut u8,
            mem::size_of::<T>() * count,
        );
        reader.read_exact(buffer)?;
    }
    Ok(vec)
}

pub fn write<T>(value: &T, writer: &mut impl Write) -> Result<()> {
    unsafe {
        let ptr = value as *const T;
        let buffer: &[u8] = std::slice::from_raw_parts_mut(
            ptr as *mut u8,
            mem::size_of::<T>(),
        );
        writer.write(buffer)?;
        return Ok(());
    };
}

pub fn write_arr<T>(value: &[T], writer: &mut impl Write) -> Result<()> {
    unsafe {
        let ptr = value as *const [T];
        let buffer: &[u8] = std::slice::from_raw_parts_mut(
            ptr as *mut u8,
            mem::size_of::<T>() * value.len(),
        );
        writer.write(buffer)?;
        return Ok(());
    };
}

pub fn write_str(value: &str, writer:  &mut (impl Write + Seek)) -> Result<()> {
    let pos = writer.stream_position()?;
    writer.seek(std::io::SeekFrom::Start(pos + 4))?;

    let mut len: i32 = 0;
    value.encode_utf16().try_for_each(|c| {
        len += 1;
        writer.write_all(&c.to_le_bytes())
    })?;

    // Set string len in the header
    let pos_end = writer.stream_position()?;
    writer.seek(std::io::SeekFrom::Start(pos))?;
    writer.write_all(&len.to_le_bytes())?;
    writer.seek(std::io::SeekFrom::Start(pos_end))?;

    Ok(())
}

