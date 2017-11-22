struct UserProfile {
    1:i64 update_time
    2:map<string, double> kw_map,
    3:map<string, double> tag_map,
    4:map<string, double> topic_map,
    5:map<string, double> account_map,
    6:map<string, double> title_map,
    7:map<string, double> other_map
}

struct LongTermUserProfile {
    1:required string mid,
    2:i64 update_time
    3:UserProfile pos_info,
    4:UserProfile neg_info
}


struct HotFeature {
    1:i32 op_time=0,  // 插入、更新时间
    2:i32 weixin_read_num=0,  //微信阅读数
    3:i32 app_read_num=0, //app内阅读数
    4:i32 app_show_num=0, //app内展示数
    5:i32 app_read_duration=0,    //app内阅读时长
    6:i32 app_favor_num=0,    //app内点赞数
    7:i32 app_collect_num=0,  //app内收藏数
    8:i32 app_share_num=0,    //app内分享数
    9:i32 news_sogourank_pv=0, //sogourank pv
    10:i32 news_comment_num=0, //评论数
    11:i32 news_participant_num=0, //参与数
    12:i32 sogou_search_index=0
}
