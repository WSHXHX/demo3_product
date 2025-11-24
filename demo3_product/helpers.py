from itertools import product
from lxml import html


def generate_variants_and_options(data, pk=None, prices=None, ik=None, images=None, default_price=None):
    # 准备列表和对应的名称、位置
    # --- 1. 先按照 position 排序 ---
    sorted_items = sorted(data.items(), key=lambda x: x[1]['position'])
    attr_names = [k for k, v in sorted_items]
    attr_values = [v['val'] for k, v in sorted_items]

    # --- 2. 生成 options 数组 ---
    options = [
        {
            "name": name,
            "position": info["position"],
            "values": info["val"]
        }
        for name, info in sorted_items
    ]

    variants = []
    # --- 3. 做全组合 ---
    for combo in product(*attr_values):
        combo_dict = dict(zip(attr_names, combo))

        # attributes string: "red / S"
        attributes_str = " / ".join(combo)

        # --- option1, option2, ...
        option_fields = {f"option{i + 1}": combo[i] for i in range(len(combo))}

        # --- price 逻辑 ---
        if pk:
            key_value = combo_dict.get(pk)
            price = prices.get(key_value) if prices else None
        else:
            price = default_price

        # --- image 逻辑 ---
        if ik:
            key_value = combo_dict.get(ik)
            image_id = images.get(key_value) if images else None
        else:
            image_id = None

        variant = {
            "title": attributes_str,
            "price": price,
            "weght": "",
            "barcode": "",
            "curreny": "",
            "image_id": image_id,
            "position": len(variants) + 1,
            "weght_unit": "",
            "compare_at_price": None
        }

        # 添加 option1、option2…
        variant.update(option_fields)

        variants.append(variant)

    return variants, options


def clean_html_structure(html_string: str) -> str:
    """
    Remove all attributes from the HTML while keeping tags and text.
    Input: an HTML string
    Output: cleaned HTML string
    """
    if not html_string: return ""

    # Parse HTML fragment
    try: node = html.fromstring(html_string)
    except Exception: return html_string  # invalid html, return original

    def strip_attributes(element):
        # Remove attributes on current node
        element.attrib.clear()

        # Recursively process children
        for child in element: strip_attributes(child)

    strip_attributes(node)

    # Return cleaned HTML
    return html.tostring(node, encoding="unicode")
