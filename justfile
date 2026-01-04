set shell := ["powershell.exe", "-c"]

debug:
    cargo build
    cp "F:\Tauri\jj-with-cdc\target\debug\jj.exe" "E:\Program Files\Rust\.cargo\bin\jjd.exe"

release:
    cargo build --release
    cp "F:\Tauri\jj-with-cdc\target\release\jj.exe" "E:\Program Files\Rust\.cargo\bin\jj.exe"

remove:
    rm "E:\Program Files\Rust\.cargo\bin\jjd.exe"
