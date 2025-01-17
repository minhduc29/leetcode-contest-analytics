def contest_ranking_query(page):
    """Json argument in request for contest ranking query"""
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


def contest_problems_query(page_num):
    """Json argument in request for contest problems query"""
    query = f"""
            query {{
                pastContests(pageNo: {page_num}, numPerPage: 10) {{
                    data {{
                        titleSlug
                        questions {{
                            titleSlug
                        }}
                    }}
                }}
            }}"""
    return {"query": query}


def problem_tags_query(title):
    """Json argument in request for problem tags query"""
    query = f"""
            query {{
                question(titleSlug: "{title}") {{
                    topicTags {{
                        name
                    }}
                }}
            }}"""
    return {"query": query}
