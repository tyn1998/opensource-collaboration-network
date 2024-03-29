import networkx as nx
from itertools import combinations


def harmonic_mean(weights, ratio=0.01):
    """计算权重的调和平均值。"""
    return ratio * len(weights) / sum(1.0 / w for w in weights) if weights else 0


def filter_top_edges(G, max=500000):
    """只保留权重在前percentile%的边"""
    weights = [data['weight'] for _, _, data in G.edges(data=True)]
    if not weights:
        return G  # 如果没有边，直接返回

    # 确定权重的阈值
    threshold = sorted(weights, reverse=True)[max]

    # 删除权重低于阈值的边
    edges_to_remove = [(u, v) for u, v, data in G.edges(data=True) if data['weight'] < threshold]
    G.remove_edges_from(edges_to_remove)

    # 删除孤立节点
    isolated_nodes = list(nx.isolates(G))
    G.remove_nodes_from(isolated_nodes)

    print(f"Removed {len(edges_to_remove)} edges. Now {G.number_of_edges()} edges remain.")
    return G


# 从 repo-actor 异质网络生成两个同质网络 repo-repo 和 actor-actor
def gen_rr_aa_from_ra(G_ra):
    # 初始化同质图
    G_rr = nx.Graph()  # 使用无向图
    G_aa = nx.Graph()  # 修改为无向图
    print("初始化同质网络完成。")

    print("开始构建repo_repo网络...")
    actor_to_repos = {}
    for u, v, data in G_ra.edges(data=True):
        if G_ra.nodes[u]['type'] == 'actor' and G_ra.nodes[v]['type'] == 'repo':
            actor_to_repos.setdefault(u, []).append((v, data['weight']))
        elif G_ra.nodes[u]['type'] == 'repo' and G_ra.nodes[v]['type'] == 'repo':
            base_weight = G_rr.get_edge_data(u, v, default={'weight': 0})['weight']
            G_rr.add_edge(u, v, weight=base_weight + data['weight'])

    for actor, repos_weights in actor_to_repos.items():
        for (repo1, weight1), (repo2, weight2) in combinations(repos_weights, 2):
            weight = harmonic_mean([weight1, weight2])
            base_weight = G_rr.get_edge_data(repo1, repo2, default={'weight': 0})['weight']
            G_rr.add_edge(repo1, repo2, weight=base_weight + weight)

    # 构建repo_repo网络后调用filter_top_edges函数
    # print("开始过滤repo_repo网络，只保留权重前20%的边...")
    # G_rr = filter_top_edges(G_rr, percentile=20)
    print("repo_repo网络构建完成。")

    print("开始构建actor_actor网络...")
    actor_to_repo_weights = {}
    for u, v, data in G_ra.edges(data=True):
        if G_ra.nodes[u]['type'] == 'actor' and G_ra.nodes[v]['type'] == 'repo':
            actor_to_repo_weights.setdefault(u, {}).setdefault(v, 0)
            actor_to_repo_weights[u][v] += data['weight']

    for actor1, repos_weights1 in actor_to_repo_weights.items():
        for actor2, repos_weights2 in actor_to_repo_weights.items():
            if actor1 != actor2:
                common_repos = set(repos_weights1.keys()) & set(repos_weights2.keys())
                total_weight = sum(harmonic_mean([repos_weights1[repo], repos_weights2[repo]]) for repo in common_repos)
                if total_weight > 0:
                    base_weight = G_aa.get_edge_data(actor1, actor2, default={'weight': 0})['weight']
                    G_aa.add_edge(actor1, actor2, weight=base_weight + total_weight)

    if G_aa.number_of_edges() > 500000:
        print("开始过滤actor_actor网络，只保留权重前500000的边...")
        G_aa = filter_top_edges(G_aa, max=500000)
    print("actor_actor网络构建完成。")

    return G_rr, G_aa
