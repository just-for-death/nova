const js = require("@eslint/js");
const globals = require("globals");

module.exports = [
    js.configs.recommended,
    {
        files: ["server.js", "monitor.js", "eslint.config.js"],
        languageOptions: {
            ecmaVersion: 2022,
            sourceType: "commonjs",
            globals: {
                ...globals.node,
                __dirname: "readonly",
                process: "readonly"
            }
        },
        rules: {
            "no-undef": "error",
            "no-unused-vars": ["warn", { "argsIgnorePattern": "^_", "varsIgnorePattern": "^_" }],
            "no-empty": "warn"
        }
    },
    {
        files: ["public/sw.js"],
        languageOptions: {
            globals: {
                ...globals.serviceworker
            }
        },
        rules: {
            "no-undef": "off"
        }
    }
];
