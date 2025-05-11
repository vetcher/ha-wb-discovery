from io import TextIOWrapper
import json
import os

import yaml

def get_test_dirs():
    test_dir = os.path.join('tests/testdata')
    return [os.path.join(test_dir, d) for d in os.listdir(test_dir)]

def preprocess_schema_lines(line: str, replacers: dict[str, str]) -> str:
    for old, new in replacers.items():
        line = line.replace(old, new)
    return line

def write_schema(docs: TextIOWrapper):
    with open("ha_wb_discovery/config.py", "r") as f:
        lines = f.readlines()
        found_schema = False
        for line in lines:
            if line.startswith("def config_schema_builder"):
                found_schema = True
                docs.write(f"```python\n")
                docs.write(line)
                continue
            if found_schema:
                line = preprocess_schema_lines(
                    line,
                    {
                        "Coerce(ConfigLogLevel)": "DEBUG | INFO | WARNING | WARN | ERROR | FATAL",
                        "__invalid_qos_msg": '"Invalid QoS: must be 0, 1 or 2"',
                    },
                )
                docs.write(line)
        if found_schema:
            docs.write(f"```\n\n")

def main():
    with open("DOCS.md", "w") as docs:
        docs.write("# Config parameters\n\n")
        write_schema(docs)
        docs.write("# Config examples\n\n")
        if os.path.exists('docs/config_examples.md'):
            with open('docs/config_examples.md', 'r') as f:
                docs.write(f.read())
                docs.write("\n")
        for test_dir in get_test_dirs():
            options_file = os.path.join(test_dir, 'options.json')
            if os.path.exists(options_file):
                docs.write(f"[{options_file}](https://github.com/vetcher/ha-wb-discovery/blob/main/{options_file})\n\n")
                with open(options_file, 'r') as f:
                    options = json.load(f)
                    yaml_options = yaml.dump(options)
                    docs.write(f"```yaml\n")
                    docs.write(yaml_options)
                    docs.write(f"```\n\n")

        with open("docs/troubleshooting.md", "r") as ts:
            docs.write(ts.read())

if __name__ == "__main__":
    main()