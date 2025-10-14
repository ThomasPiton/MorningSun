class StockEsgRiskExtractor(BaseClient):

    def __init__(self):
        super().__init__(auth_type=AuthType.API_KEY)

    def get(
        self,
        id_sec: str,
    ) -> pd.DataFrame:
    
        url = f"{URLS["stock_esg_risk"]}/{id_sec}/data"
        
        params = {
            "reportType":"A",
            'languageId': 'eg',
            'locale': 'eg',
            'clientId': 'MDC',
            "benchmarkId": "undefined",
            'component': 'sal-eqsv-risk-rating-breakdown',
            'version': '4.69.0'
        }

        response = self.request(url=url, params=params, method="GET")
        
        if not response or len(response) == 0:
            return pd.DataFrame()
        
        # Flatten main dictionary
        flat_data = {k: v for k, v in response.items() if k != "peers"}

        # Convert main dict to vertical DataFrame
        df_main = pd.DataFrame(list(flat_data.items()), columns=["metric", "value"]).set_index("metric")

        # Flatten peers
        dfs_peers = []
        for i, peer in enumerate(response.get("peers", []), start=1):
            # Prefix each metric with "peer{index}_"
            peer_flat = {f"peer{i}_{k}": v for k, v in peer.items()}
            df_peer = pd.DataFrame(list(peer_flat.items()), columns=["metric", "value"]).set_index("metric")
            dfs_peers.append(df_peer)

        # Combine main + peers
        df = pd.concat([df_main] + dfs_peers)
        
        return df