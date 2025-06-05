// SPDX-License-Identifier: MPL-2.0

use alloc::{boxed::Box, vec::Vec};

use font8x8::UnicodeFonts;

#[derive(Debug)]
pub struct BitmapFont {
    width: usize,
    height: usize,
    char_size: usize,
    bitmap: Box<[u8]>,
}

impl BitmapFont {
    pub fn new(width: usize, height: usize, bitmap: Box<[u8]>) -> Self {
        let row_size = width.div_ceil(u8::BITS as usize);
        let char_size = row_size.checked_mul(height).unwrap();

        assert_ne!(bitmap.len(), 0);
        assert_eq!(bitmap.len() % char_size, 0);

        Self {
            width,
            height,
            char_size,
            bitmap,
        }
    }

    pub fn new_basic8x8() -> Self {
        const CHAR_COUNT: u32 = 0x7F;

        const FONT_WIDTH: usize = 8;
        const FONT_HEIGHT: usize = 8;

        let bitmap = (0..CHAR_COUNT)
            .flat_map(|ch| {
                font8x8::BASIC_FONTS
                    .get(char::from_u32(ch).unwrap())
                    .unwrap()
                    .into_iter()
            })
            .collect();

        Self::new(FONT_WIDTH, FONT_HEIGHT, bitmap)
    }

    pub fn new_with_vpitch(
        width: usize,
        height: usize,
        vpitch: usize,
        mut bitmap: Vec<u8>,
    ) -> Self {
        if vpitch == height {
            return Self::new(width, height, bitmap.into_boxed_slice());
        }
        assert!(height < vpitch);

        let row_size = width.div_ceil(u8::BITS as usize);
        let char_size_old = row_size.checked_mul(vpitch).unwrap();
        let char_size_new = row_size.checked_mul(height).unwrap();

        assert_ne!(bitmap.len(), 0);
        assert_eq!(bitmap.len() % char_size_old, 0);

        let mut old_pos = char_size_old;
        let mut new_pos = char_size_new;
        while old_pos < bitmap.len() {
            bitmap.copy_within(old_pos..old_pos + char_size_new, new_pos);
            old_pos += char_size_old;
            new_pos += char_size_new;
        }

        bitmap.truncate(new_pos);

        Self::new(width, height, bitmap.into_boxed_slice())
    }

    pub fn width(&self) -> usize {
        self.width
    }

    pub fn height(&self) -> usize {
        self.height
    }

    pub fn char(&self, ch: u8) -> Option<BitmapChar> {
        let pos = (ch as usize) * self.char_size;
        let data = self.bitmap.get(pos..pos + self.char_size)?;

        Some(BitmapChar {
            font: self,
            char_data: data,
        })
    }
}

#[derive(Debug)]
pub struct BitmapChar<'a> {
    font: &'a BitmapFont,
    char_data: &'a [u8],
}

impl<'a> BitmapChar<'a> {
    pub fn rows(&self) -> impl Iterator<Item = BitmapCharRow<'a>> {
        let row_size = self.font.width.div_ceil(u8::BITS as usize);
        self.char_data
            .chunks_exact(row_size)
            .map(|chunk| BitmapCharRow {
                font: self.font,
                row_data: chunk,
            })
    }
}

#[derive(Debug)]
pub struct BitmapCharRow<'a> {
    font: &'a BitmapFont,
    row_data: &'a [u8],
}

impl BitmapCharRow<'_> {
    pub fn bits(&self) -> impl Iterator<Item = bool> + '_ {
        (0..self.font.width).map(|i| {
            let nbyte = i / (u8::BITS as usize);
            let nbit = i % (u8::BITS as usize);
            self.row_data[nbyte] & (1 << nbit) != 0
        })
    }
}
