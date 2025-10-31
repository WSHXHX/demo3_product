import itertools

def generate_variants_and_options(colors=None, sizes=None, lengths=None):
    # 准备列表和对应的名称、位置
    lists_info = []
    if colors:
        lists_info.append(("color", colors, len(lists_info) + 1))
    if sizes:
        lists_info.append(("size", sizes, len(lists_info) + 1))
    if lengths:
        lists_info.append(("length", lengths, len(lists_info) + 1))

    # 如果没有列表，返回空结果
    if not lists_info:
        return [], []

    # 提取列表用于组合
    lists_for_combination = [info[1] for info in lists_info]

    # 生成所有组合
    combinations = list(itertools.product(*lists_for_combination))

    # 生成变体列表
    variants = []
    for combo in combinations:
        item = {}
        title_parts = []

        for i, (name, values, position) in enumerate(lists_info):
            item[f"option{i + 1}"] = combo[i]
            title_parts.append(str(combo[i]))

        item["title"] = " / ".join(title_parts)
        variants.append(item)

    # 生成选项配置
    options = []
    for name, values, position in lists_info:
        options.append({
            "name": name,
            "values": values,
            "position": position
        })

    return variants, options
