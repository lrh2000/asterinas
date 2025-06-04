// SPDX-License-Identifier: MPL-2.0

use alloc::{sync::Arc, vec::Vec};

use aster_console::{AnyConsoleDevice, ConsoleCallback};
use font8x8::UnicodeFonts;
use ostd::{
    sync::{LocalIrqDisabled, SpinLock},
    Error, Result,
};
use spin::Once;

use crate::{
    ansi_escape::{EscapeFsm, EscapeOp},
    FrameBuffer, Pixel, FRAMEBUFFER,
};

/// The font width in pixels when using `font8x8`.
const FONT_WIDTH: usize = 8;

/// The font height in pixels when using `font8x8`.
const FONT_HEIGHT: usize = 8;

/// A text console rendered onto the framebuffer.
#[derive(Debug)]
pub struct FramebufferConsole {
    inner: SpinLock<(ConsoleState, EscapeFsm), LocalIrqDisabled>,
}

pub const CONSOLE_NAME: &str = "Framebuffer-Console";

pub static FRAMEBUFFER_CONSOLE: Once<Arc<FramebufferConsole>> = Once::new();

pub(crate) fn init() {
    let Some(fb) = FRAMEBUFFER.get() else {
        log::warn!("Framebuffer not initialized");
        return;
    };

    FRAMEBUFFER_CONSOLE.call_once(|| Arc::new(FramebufferConsole::new(fb.clone())));
}

impl AnyConsoleDevice for FramebufferConsole {
    fn send(&self, buf: &[u8]) {
        let mut inner = self.inner.lock();
        let (state, esc_fsm) = &mut *inner;

        for byte in buf {
            if esc_fsm.eat(*byte, state) {
                // The character is part of an ANSI escape sequence.
                continue;
            }

            if *byte == 0 {
                // The character is a NUL character.
                continue;
            }

            let c = char::from_u32(*byte as u32).unwrap();
            state.send_char(c);
        }
    }

    fn register_callback(&self, _: &'static ConsoleCallback) {
        // Unsupported, do nothing.
    }
}

impl FramebufferConsole {
    /// Creates a new framebuffer console.
    pub(self) fn new(framebuffer: Arc<FrameBuffer>) -> Self {
        let state = ConsoleState {
            x_pos: 0,
            y_pos: 0,
            fg_color: Pixel::WHITE,
            bg_color: Pixel::BLACK,
            bytes: alloc::vec![0u8; framebuffer.size()],
            backend: framebuffer,
        };

        let esc_fsm = EscapeFsm::new();

        Self {
            inner: SpinLock::new((state, esc_fsm)),
        }
    }
}

#[derive(Debug)]
pub(super) struct ConsoleState {
    x_pos: usize,
    y_pos: usize,
    fg_color: Pixel,
    bg_color: Pixel,
    bytes: Vec<u8>,
    backend: Arc<FrameBuffer>,
}

impl ConsoleState {
    fn carriage_return(&mut self) {
        self.x_pos = 0;
    }

    fn newline(&mut self) {
        if self.y_pos >= self.backend.height() - FONT_HEIGHT {
            self.shift_lines_up();
        }
        self.y_pos += FONT_HEIGHT;
        self.x_pos = 0;
    }

    fn shift_lines_up(&mut self) {
        let offset = self.backend.calc_offset(0, FONT_HEIGHT).as_usize();
        self.bytes.copy_within(offset.., 0);
        self.bytes[self.backend.size() - offset..].fill(0);
        self.backend.write_bytes_at(0, &self.bytes).unwrap();
        self.y_pos -= FONT_HEIGHT;
    }

    /// Sends a single character to be drawn on the framebuffer.
    ///
    /// # Panics
    ///
    /// This method will panic if the character is not one of
    /// the Basic Latin characters (`U+0000` - `U+007F`).
    pub(self) fn send_char(&mut self, c: char) {
        if c == '\n' {
            self.newline();
            return;
        } else if c == '\r' {
            self.carriage_return();
            return;
        }

        if self.x_pos + FONT_WIDTH > self.backend.width() {
            self.newline();
        }

        let rendered = font8x8::BASIC_FONTS
            .get(c)
            .expect("character not found in basic font");
        let fg_pixel = self.backend.render_pixel(self.fg_color);
        let bg_pixel = self.backend.render_pixel(self.bg_color);
        let mut offset = self.backend.calc_offset(self.x_pos, self.y_pos);
        for byte in rendered.iter() {
            for bit in 0..8 {
                let on = *byte & (1 << bit) != 0;
                let pixel = if on { fg_pixel } else { bg_pixel };

                // Cache the rendered pixel
                self.bytes[offset.as_usize()..offset.as_usize() + pixel.nbytes()]
                    .copy_from_slice(pixel.as_slice());
                // Write the pixel to the framebuffer
                self.backend.write_pixel_at(offset, pixel).unwrap();

                offset.x_add(1);
            }
            offset.x_add(-(FONT_WIDTH as isize));
            offset.y_add(1);
        }
        self.x_pos += FONT_WIDTH;
    }
}

impl EscapeOp for ConsoleState {
    fn set_cursor(&mut self, x: usize, y: usize) -> Result<()> {
        let x_pos = x.checked_mul(FONT_WIDTH).ok_or(Error::InvalidArgs)?;
        let y_pos = y.checked_mul(FONT_HEIGHT).ok_or(Error::InvalidArgs)?;

        if x_pos > self.backend.width() - FONT_WIDTH || y_pos > self.backend.height() - FONT_HEIGHT
        {
            return Err(Error::InvalidArgs);
        }

        self.x_pos = x_pos;
        self.y_pos = y_pos;

        Ok(())
    }

    fn set_fg_color(&mut self, val: Pixel) {
        self.fg_color = val;
    }

    fn set_bg_color(&mut self, val: Pixel) {
        self.bg_color = val;
    }
}
