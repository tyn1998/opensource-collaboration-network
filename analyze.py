import pandas as pd
import networkx as nx


def stats_from_events_csv(events_csv_path):
    # 读取CSV文件
    df = pd.read_csv(events_csv_path)

    # 初始化结果字典
    stats = {
        'record_count': 0,
        'type_counts': {},
        'actor_count': 0,
        'org_count': 0,
        'repo_count': 0,
        'hourly_distribution': {}
    }

    # 记录条数
    stats['record_count'] = len(df)

    # 各个type出现的次数
    stats['type_counts'] = df['type'].value_counts().to_dict()

    # actor个数
    stats['actor_count'] = df['actor_id'].nunique()

    # org个数
    stats['org_count'] = df['org_id'].nunique()

    # repo个数
    stats['repo_count'] = df['repo_id'].nunique()

    # 24小时分布情况
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['hour'] = df['created_at'].dt.hour
    stats['hourly_distribution'] = df['hour'].value_counts().sort_index().to_dict()

    return stats


def stats_from_gml(gml_path):
    # 读取GML文件生成图
    G = nx.read_gml(gml_path)

    # 初始化统计字典
    stats = {
        'node_count': G.number_of_nodes(),
        'edge_count': G.number_of_edges(),
    }

    # 无论是有向图还是无向图，都计算平均度数（对于有向图，计算平均总度数）
    if nx.is_directed(G):
        stats['average_degree'] = sum(d for n, d in G.in_degree()) + sum(d for n, d in G.out_degree()) / float(
            G.number_of_nodes())
    else:
        stats['average_degree'] = sum(d for n, d in G.degree()) / float(G.number_of_nodes())

    # 图的密度，适用于有向图和无向图
    stats['density'] = nx.density(G)

    # 计算图的连通性
    if nx.is_directed(G):
        # 对于有向图，计算是否为弱连通
        stats['is_weakly_connected'] = nx.is_weakly_connected(G)
    else:
        # 对于无向图，直接计算连通性
        stats['is_connected'] = nx.is_connected(G)

    # 为所有图类型计算全局聚类系数（适用于无向图的转换）
    stats['average_clustering'] = nx.average_clustering(nx.to_undirected(G))

    # 计算图的中心性度量，例如度中心性（适用于无向图的转换）
    degree_centrality = nx.degree_centrality(nx.to_undirected(G))
    stats['average_degree_centrality'] = sum(degree_centrality.values()) / len(degree_centrality)

    return stats
