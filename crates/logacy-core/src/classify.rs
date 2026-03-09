use std::path::Path;

/// Classify a file path into a programming language based on extension/filename.
pub fn language_from_path(path: &str) -> &'static str {
    let p = Path::new(path);

    // Special-case filenames first
    if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
        match name {
            "Makefile" | "GNUmakefile" | "makefile" => return "Make",
            "Dockerfile" => return "Docker",
            "CMakeLists.txt" => return "CMake",
            "Kconfig" | "Kbuild" => return "Kbuild",
            "Vagrantfile" => return "Ruby",
            "Gemfile" => return "Ruby",
            "Rakefile" => return "Ruby",
            "Jenkinsfile" => return "Groovy",
            _ => {}
        }
    }

    match p.extension().and_then(|e| e.to_str()) {
        Some("c") | Some("h") => "C",
        Some("cpp") | Some("cxx") | Some("cc") | Some("hpp") | Some("hxx") | Some("hh") => "C++",
        Some("rs") => "Rust",
        Some("go") => "Go",
        Some("py") | Some("pyi") => "Python",
        Some("js") | Some("mjs") | Some("cjs") => "JavaScript",
        Some("ts") | Some("mts") | Some("cts") => "TypeScript",
        Some("jsx") => "JSX",
        Some("tsx") => "TSX",
        Some("java") => "Java",
        Some("kt") | Some("kts") => "Kotlin",
        Some("scala") | Some("sc") => "Scala",
        Some("rb") => "Ruby",
        Some("php") => "PHP",
        Some("swift") => "Swift",
        Some("m") | Some("mm") => "Objective-C",
        Some("cs") => "C#",
        Some("fs") | Some("fsx") => "F#",
        Some("hs") | Some("lhs") => "Haskell",
        Some("ml") | Some("mli") => "OCaml",
        Some("ex") | Some("exs") => "Elixir",
        Some("erl") | Some("hrl") => "Erlang",
        Some("clj") | Some("cljs") | Some("cljc") => "Clojure",
        Some("lua") => "Lua",
        Some("pl") | Some("pm") => "Perl",
        Some("r") | Some("R") => "R",
        Some("jl") => "Julia",
        Some("sh") | Some("bash") | Some("zsh") => "Shell",
        Some("ps1") | Some("psm1") => "PowerShell",
        Some("bat") | Some("cmd") => "Batch",
        Some("sql") => "SQL",
        Some("html") | Some("htm") => "HTML",
        Some("css") => "CSS",
        Some("scss") | Some("sass") => "Sass",
        Some("less") => "Less",
        Some("xml") | Some("xsl") | Some("xsd") => "XML",
        Some("json") => "JSON",
        Some("yaml") | Some("yml") => "YAML",
        Some("toml") => "TOML",
        Some("ini") | Some("cfg") => "INI",
        Some("proto") => "Protobuf",
        Some("thrift") => "Thrift",
        Some("graphql") | Some("gql") => "GraphQL",
        Some("dart") => "Dart",
        Some("zig") => "Zig",
        Some("nim") => "Nim",
        Some("v") => "V",
        Some("d") => "D",
        Some("ada") | Some("adb") | Some("ads") => "Ada",
        Some("f") | Some("f90") | Some("f95") | Some("for") => "Fortran",
        Some("asm") | Some("s") | Some("S") => "Assembly",
        Some("cmake") => "CMake",
        Some("am") | Some("in") | Some("ac") | Some("m4") => "Autotools",
        Some("spec") => "RPM Spec",
        Some("md") | Some("markdown") => "Markdown",
        Some("rst") => "reStructuredText",
        Some("adoc") | Some("asciidoc") => "AsciiDoc",
        Some("txt") => "Text",
        Some("tex") | Some("sty") => "LaTeX",
        _ => "Other",
    }
}

/// Classify a file path into a category: test, docs, build, or source.
pub fn category_from_path(path: &str) -> &'static str {
    let lower = path.to_ascii_lowercase();

    // Test detection
    if lower.contains("/test/")
        || lower.contains("/tests/")
        || lower.contains("_test.")
        || lower.contains(".test.")
        || lower.contains("_spec.")
        || lower.contains(".spec.")
    {
        return "test";
    }
    // Check filename prefix
    if let Some(name) = Path::new(&lower).file_name().and_then(|n| n.to_str()) {
        if name.starts_with("test_") {
            return "test";
        }
    }

    // Docs detection
    if lower.contains("/doc/")
        || lower.contains("/docs/")
        || lower.contains("/documentation/")
    {
        return "docs";
    }
    match Path::new(path).extension().and_then(|e| e.to_str()) {
        Some("md") | Some("markdown") | Some("rst") | Some("adoc") | Some("asciidoc") | Some("txt") => {
            return "docs";
        }
        _ => {}
    }

    // Build detection
    if let Some(name) = Path::new(path).file_name().and_then(|n| n.to_str()) {
        match name {
            "Makefile" | "GNUmakefile" | "makefile" | "CMakeLists.txt" | "configure"
            | "Cargo.toml" | "Cargo.lock" | "package.json" | "package-lock.json"
            | "yarn.lock" | "pnpm-lock.yaml" | "Gemfile" | "Gemfile.lock"
            | "build.gradle" | "pom.xml" | "go.mod" | "go.sum"
            | "Kconfig" | "Kbuild" => {
                return "build";
            }
            _ => {}
        }
    }
    match Path::new(path).extension().and_then(|e| e.to_str()) {
        Some("am") | Some("in") | Some("ac") | Some("cmake") | Some("m4") | Some("spec") => {
            return "build";
        }
        _ => {}
    }

    "source"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_from_path() {
        assert_eq!(language_from_path("src/main.rs"), "Rust");
        assert_eq!(language_from_path("lib/foo.c"), "C");
        assert_eq!(language_from_path("include/bar.h"), "C");
        assert_eq!(language_from_path("Makefile"), "Make");
        assert_eq!(language_from_path("scripts/build.sh"), "Shell");
        assert_eq!(language_from_path("unknown.xyz"), "Other");
        assert_eq!(language_from_path("Dockerfile"), "Docker");
        assert_eq!(language_from_path("src/autoconf/config.ac"), "Autotools");
    }

    #[test]
    fn test_category_from_path() {
        assert_eq!(category_from_path("src/main.rs"), "source");
        assert_eq!(category_from_path("tests/test_foo.py"), "test");
        assert_eq!(category_from_path("src/foo_test.go"), "test");
        assert_eq!(category_from_path("test_helper.py"), "test");
        assert_eq!(category_from_path("docs/guide.md"), "docs");
        assert_eq!(category_from_path("README.md"), "docs");
        assert_eq!(category_from_path("CHANGELOG.txt"), "docs");
        assert_eq!(category_from_path("Makefile"), "build");
        assert_eq!(category_from_path("configure.ac"), "build");
        assert_eq!(category_from_path("Cargo.toml"), "build");
        assert_eq!(category_from_path("autoMakefile.am"), "build");
    }
}
