# Description: 从GitHub事件数据中构建一个有向图，其中包含了actor和repo节点，以及贡献类和引用类边。

import pandas as pd
import networkx as nx
import re


def determine_contribution_weight(event_type, action, pull_merged):
    if event_type == 'IssueCommentEvent':
        return 1
    elif event_type == 'IssuesEvent' and action == 'opened':
        return 2
    elif event_type == 'PullRequestEvent':
        if action == 'opened':
            return 3
        elif action == 'closed' and pull_merged:
            return 5
    elif event_type == 'PullRequestReviewCommentEvent':
        return 4
    return 0


def extract_references(body):
    if pd.isna(body):
        return [], []
    mention_pattern = r"@([a-zA-Z0-9_-]+)"
    repo_ref_pattern = r"github\.com/([a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+)"
    mentions = re.findall(mention_pattern, body)
    repo_refs = re.findall(repo_ref_pattern, body)
    return mentions, repo_refs


def trim_graph_edges(G, max_edges=500000):
    # 筛选贡献边
    contribution_edges = [(u, v, d) for u, v, d in G.edges(data=True) if d.get('type') == 'contribution']

    # 如果贡献边数量超过限制，则进行削减
    if len(contribution_edges) > max_edges:
        # 根据权重排序边，保留权重最高的边
        sorted_edges = sorted(contribution_edges, key=lambda x: x[2]['weight'], reverse=True)
        edges_to_keep = set(sorted_edges[:max_edges])

        # 删除不在保留列表中的边
        for edge in contribution_edges:
            if edge not in edges_to_keep:
                G.remove_edge(edge[0], edge[1])

        # 删除孤立节点
        isolated_nodes = list(nx.isolates(G))
        G.remove_nodes_from(isolated_nodes)

        print(f"Graph trimmed to top {max_edges} contribution edges. Removed {len(isolated_nodes)} isolated nodes.")


# generate repo-actor network from GitHub events
def gen_ra_from_events(events_csv_path):
    chunksize = 10000  # 根据您的内存限制调整
    G = nx.DiGraph()

    # 初始化存储actors和repos信息的字典
    actors_info = {}
    repos_info = {}

    print("开始收集节点信息...")
    for chunk in pd.read_csv(events_csv_path, chunksize=chunksize,
                             usecols=['actor_id', 'actor_login', 'repo_id', 'repo_name', 'org_id', 'org_login',
                                      'created_at']):
        for _, row in chunk.iterrows():
            actor_id, repo_id = str(row['actor_id']), str(row['repo_id'])
            created_at = pd.to_datetime(row['created_at'])

            # 更新actor信息，如果这是一个更晚的记录
            if actor_id not in actors_info or created_at > actors_info[actor_id]['latest']:
                actors_info[actor_id] = {
                    'login': row['actor_login'],
                    'latest': created_at
                }

            # 更新repo信息，如果这是一个更晚的记录
            if repo_id not in repos_info or created_at > repos_info[repo_id]['latest']:
                repos_info[repo_id] = {
                    'name': row['repo_name'],
                    'org_id': str(row['org_id']),
                    'org_login': row['org_login'],
                    'latest': created_at
                }
        print(f"已处理 {chunk.shape[0]} 行数据...")

    # 创建节点，不包括'latest'属性
    for actor_id, attrs in actors_info.items():
        G.add_node(actor_id, type='actor', login=attrs['login'])

    for repo_id, attrs in repos_info.items():
        G.add_node(repo_id, type='repo', name=attrs['name'], org_id=attrs['org_id'], org_login=attrs['org_login'])

    print(f"总共收集到 {len(actors_info)} 个独特的 actor 节点和 {len(repos_info)} 个独特的 repo 节点。")

    print("开始处理边的添加...")
    # 我们需要创建一个从 login 到 actor_id 的映射
    login_to_actor_id = {info['login']: actor_id for actor_id, info in actors_info.items()}
    # 我们需要创建一个从 repo_name 到 repo_id 的映射
    name_to_repo_id = {info['name']: repo_id for repo_id, info in repos_info.items()}

    for chunk in pd.read_csv(events_csv_path, chunksize=chunksize):
        for index, row in chunk.iterrows():
            # 在这里添加逻辑来处理每行数据，包括添加贡献类边和引用类边
            # 确保在添加边之前，边的两端节点都已经存在于G中

            # 添加贡献边
            actor_id, repo_id = str(row['actor_id']), str(row['repo_id'])
            weight = determine_contribution_weight(row['type'], row['action'], row['pull_merged'])
            if weight > 0:
                # 如果边已存在，更新权重
                if G.has_edge(actor_id, repo_id):
                    G[actor_id][repo_id]['weight'] += weight
                else:
                    G.add_edge(actor_id, repo_id, weight=weight, type='contribution')

            # 添加引用边
            mentions, repo_refs = extract_references(row['body'] if pd.notna(row['body']) else "")
            actor_id_str = str(row['actor_id'])  # 将actor_id转换为字符串
            # 添加mention边（actor到actor）
            for mention in mentions:
                if mention in login_to_actor_id:
                    mentioned_actor_id_str = login_to_actor_id[mention]
                    # 检查是否避免自引用且两个节点都存在于G中
                    if actor_id_str != mentioned_actor_id_str and G.has_node(actor_id_str) and G.has_node(mentioned_actor_id_str):
                        if G.has_edge(actor_id_str, mentioned_actor_id_str):
                            G[actor_id_str][mentioned_actor_id_str]['weight'] += 2  # 累计权重
                        else:
                            G.add_edge(actor_id_str, mentioned_actor_id_str, weight=2, type='mention')

            # 添加repo引用边（repo到repo）
            repo_id_str = str(row['repo_id'])  # 将repo_id转换为字符串
            for repo_ref in repo_refs:
                referenced_repo_name = '/'.join(repo_ref.split('/')[-2:])  # 提取完整的repo_name
                if referenced_repo_name in name_to_repo_id:
                    referenced_repo_id_str = name_to_repo_id[referenced_repo_name]
                    # 检查是否避免自引用且两个节点都存在于G中
                    if repo_id_str != referenced_repo_id_str and G.has_node(repo_id_str) and G.has_node(referenced_repo_id_str):
                        if G.has_edge(repo_id_str, referenced_repo_id_str):
                            G[repo_id_str][referenced_repo_id_str]['weight'] += 3  # 累计权重
                        else:
                            G.add_edge(repo_id_str, referenced_repo_id_str, weight=3, type='repo_reference')

        print(f"已处理 {chunk.shape[0]} 行数据...")

    contribution_edges_count = sum(1 for _, _, edge_data in G.edges(data=True) if edge_data.get('type') == 'contribution')
    mention_edges_count = sum(1 for _, _, edge_data in G.edges(data=True) if edge_data.get('type') == 'mention')
    repo_reference_edges_count = sum(1 for _, _, edge_data in G.edges(data=True) if edge_data.get('type') == 'repo_reference')
    print(f"总共收集到 {contribution_edges_count} 个 contribution 边、 {mention_edges_count} 个 mention 边和 {repo_reference_edges_count} 个 repo_reference 边。")

    # # 转换所有属性为字符串类型
    # for node, attr in G.nodes(data=True):
    #     for key in attr:
    #         attr[key] = str(attr[key])
    # for u, v, attr in G.edges(data=True):
    #     for key in attr:
    #         attr[key] = str(attr[key])

    # 创建一个新的有向图
    G_new = nx.DiGraph()

    # 复制节点，并使用新的标识符
    for node, data in G.nodes(data=True):
        if data['type'] == 'actor':
            # 对于actor节点，使用login作为新的节点id
            G_new.add_node(data['login'], **data)
        elif data['type'] == 'repo':
            # 对于repo节点，使用name作为新的节点id
            G_new.add_node(data['name'], **data)

    # 复制边，并使用新的source和target id
    for source, target, data in G.edges(data=True):
        source_data = G.nodes[source]
        target_data = G.nodes[target]

        # 根据节点类型确定新的source和target id
        if source_data['type'] == 'actor':
            new_source_id = source_data['login']
        else:
            new_source_id = source_data['name']

        if target_data['type'] == 'actor':
            new_target_id = target_data['login']
        else:
            new_target_id = target_data['name']

        # 在新图中添加边
        G_new.add_edge(new_source_id, new_target_id, **data)

    # TODO: 找出为什么存在自环边和不属于最大连通子图的节点，然后移出下面的“补救”代码
    # 检查并删除不属于最大连通子图的节点
    weakly_connected_components = list(nx.weakly_connected_components(G_new))
    if len(weakly_connected_components) > 1:
        largest_component = max(weakly_connected_components, key=len)
        other_nodes = set(G_new.nodes()) - largest_component

        print("将被删除的不属于最大连通子图的节点有：")
        for node in other_nodes:
            print(node)

        # 删除这些节点
        G_new.remove_nodes_from(other_nodes)

    # 检查并删除有自环的节点
    self_loops = list(nx.selfloop_edges(G_new))
    if self_loops:
        print("将被删除的存在自环的节点有：")
        for u, _ in self_loops:
            print(u)

        # 删除自环边
        G_new.remove_edges_from(self_loops)

        # 如果你也想删除具有自环的节点，取消注释以下代码
        # self_loop_nodes = {u for u, v in self_loops}
        # G_new.remove_nodes_from(self_loop_nodes)

    # 在导出前，根据贡献边的数量进行削减
    trim_graph_edges(G_new)

    return G_new
