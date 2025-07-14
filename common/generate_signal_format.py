import argparse
import json
from typing import Any, Dict, List


def generate_format_definition(name: str, fields: List[Dict[str, Any]]):
    """生成信号格式定义代码"""
    code = f"    # {name} 信号格式\n"
    code += f"    {name} = [\n"

    for field in fields:
        field_type = field.get("type", "str")
        required = field.get("required", True)
        description = field.get("description", "")
        example = field.get("example", None)

        code += f"        SignalFormatField(\"{field['name']}\", {field_type}, {str(required)}, "
        code += f'"{description}", {repr(example)}),\n'

    code += "    ]\n\n"
    return code


def main():
    parser = argparse.ArgumentParser(description="生成信号格式定义代码")
    parser.add_argument("name", help="信号类型名称，如 DEX_TRADE")
    parser.add_argument("--input", "-i", help="输入JSON文件路径，包含字段定义")
    args = parser.parse_args()

    if args.input:
        with open(args.input, "r") as f:
            fields = json.load(f)
    else:
        # 交互式定义字段
        fields = []
        print(f"为 {args.name} 信号定义字段:")
        while True:
            name = input("字段名称 (留空结束): ")
            if not name:
                break

            field_type = input("字段类型 [str/int/float/bool/List[type]]: ")
            required = input("是否必需 [y/n]: ").lower() == "y"
            description = input("字段描述: ")
            example = input("示例值 (可选): ")

            field = {
                "name": name,
                "type": field_type or "str",
                "required": required,
                "description": description,
            }

            if example:
                # 尝试转换示例值为实际类型
                if field_type == "int":
                    field["example"] = int(example)
                elif field_type == "float":
                    field["example"] = float(example)
                elif field_type == "bool":
                    field["example"] = example.lower() in ("true", "yes", "y", "1")
                elif field_type.startswith("List"):
                    field["example"] = json.loads(example)
                else:
                    field["example"] = example

            fields.append(field)
            print("已添加字段")

    # 生成代码
    code = generate_format_definition(args.name, fields)
    print("\n生成的代码:")
    print(code)
    print("请将此代码添加到 signal_formats.py 文件的 SignalFormats 类中")


if __name__ == "__main__":
    main()
