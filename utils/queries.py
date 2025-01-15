def contest_ranking_query(page):
    """Returns the json argument in request for contest ranking query"""
    query = f"""
            query {{
                globalRanking(page: {page}) {{
                    rankingNodes {{
                        ranking
                        currentRating
                        currentGlobalRanking
                        dataRegion
                        user {{
                            username
                            profile {{
                                countryName
                            }}
                        }}
                    }}
                }}
            }}"""
    return {"query": query}
