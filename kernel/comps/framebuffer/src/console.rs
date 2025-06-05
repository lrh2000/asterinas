// SPDX-License-Identifier: MPL-2.0

use alloc::{sync::Arc, vec::Vec};

use aster_console::{AnyConsoleDevice, ConsoleCallback};
use ostd::{
    sync::{LocalIrqDisabled, SpinLock},
    Error, Result,
};
use spin::Once;

use crate::{
    ansi_escape::{EscapeFsm, EscapeOp},
    font::BitmapFont,
    FrameBuffer, Pixel, FRAMEBUFFER,
};

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

            state.send_char(*byte);
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
            font: BitmapFont::new_basic8x8(),
            bytes: alloc::vec![0u8; framebuffer.size()],
            backend: framebuffer,
        };

        let esc_fsm = EscapeFsm::new();

        Self {
            inner: SpinLock::new((state, esc_fsm)),
        }
    }

    /// Sets the font for the framebuffer console.
    pub fn set_font(&self, font: BitmapFont) -> Result<()> {
        self.inner.lock().0.set_font(font)
    }
}

#[derive(Debug)]
pub(super) struct ConsoleState {
    x_pos: usize,
    y_pos: usize,
    fg_color: Pixel,
    bg_color: Pixel,
    font: BitmapFont,
    bytes: Vec<u8>,
    backend: Arc<FrameBuffer>,
}

impl ConsoleState {
    /// Sends a single character to be drawn on the framebuffer.
    pub(self) fn send_char(&mut self, ch: u8) {
        if ch == b'\n' {
            self.newline();
            return;
        } else if ch == b'\r' {
            self.carriage_return();
            return;
        } else if ch == b'\x08' {
            self.backspace();
            return;
        }

        if self.x_pos > self.backend.width() - self.font.width() {
            self.newline();
        }

        self.draw_char(ch);

        self.x_pos += self.font.width();
    }

    fn newline(&mut self) {
        self.y_pos += self.font.height();
        self.x_pos = 0;

        if self.y_pos > self.backend.height() - self.font.height() {
            self.shift_lines_up();
        }
    }

    fn shift_lines_up(&mut self) {
        let offset = self.backend.calc_offset(0, self.font.height()).as_usize();
        self.bytes.copy_within(offset.., 0);
        self.bytes[self.backend.size() - offset..].fill(0);

        self.backend.write_bytes_at(0, &self.bytes).unwrap();

        self.y_pos -= self.font.height();
    }

    fn carriage_return(&mut self) {
        self.x_pos = 0;
    }

    fn backspace(&mut self) {
        if self.x_pos >= self.font.width() {
            self.x_pos -= self.font.height();
        }
        // TODO: What should we do if we're at the beginning of the line?
    }

    fn draw_char(&mut self, ch: u8) {
        let Some(font_ch) = self.font.char(ch) else {
            return;
        };

        let fg_pixel = self.backend.render_pixel(self.fg_color);
        let bg_pixel = self.backend.render_pixel(self.bg_color);

        let pixel_size = fg_pixel.nbytes();

        let mut offset = self.backend.calc_offset(self.x_pos, self.y_pos);

        for row in font_ch.rows() {
            let off_st = offset.as_usize();
            let off_ed = off_st + pixel_size * self.font.width();
            let render_buf = &mut self.bytes[off_st..off_ed];

            // Write pixels to the console buffer.
            let chunks = render_buf.chunks_exact_mut(pixel_size);
            for (chunk, is_fg) in chunks.zip(row.bits()) {
                let pixel = if is_fg { fg_pixel } else { bg_pixel };
                chunk.copy_from_slice(pixel.as_slice());
            }

            // Write pixels to the framebuffer.
            self.backend.write_bytes_at(off_st, render_buf).unwrap();

            offset.y_add(1);
        }
    }

    /// Sets the font for the framebuffer console.
    pub(self) fn set_font(&mut self, font: BitmapFont) -> Result<()> {
        if font.width() > self.backend.width() {
            return Err(Error::InvalidArgs);
        }
        if font.height() > self.backend.height() / 2 {
            return Err(Error::InvalidArgs);
        }

        self.font = font;

        if self.y_pos > self.backend.height() - self.font.height() {
            self.shift_lines_up();
        }

        Ok(())
    }
}

impl EscapeOp for ConsoleState {
    fn set_cursor(&mut self, x: usize, y: usize) -> Result<()> {
        let x_pos = x.checked_mul(self.font.width()).ok_or(Error::InvalidArgs)?;
        let y_pos = y
            .checked_mul(self.font.height())
            .ok_or(Error::InvalidArgs)?;

        if x_pos > self.backend.width() - self.font.width()
            || y_pos > self.backend.height() - self.font.height()
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
