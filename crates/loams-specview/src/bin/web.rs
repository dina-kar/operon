//! The browser entry point (built by `trunk`, see the crate README).

fn main() {
    console_error_panic_hook::set_once();
    leptos::mount::mount_to_body(loams_specview::ui::App);
}
