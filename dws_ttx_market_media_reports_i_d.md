```
-- auto-generated definition
CREATE TABLE dws.dws_ttx_market_media_reports_i_d
(
    stat_date VARCHAR(MAX
) ,
    stat_hour bigint(19),
    channel VARCHAR(MAX),
    agent VARCHAR(MAX),
    account VARCHAR(MAX),
    ad_plan_id_str VARCHAR(MAX),
    ad_plan_name VARCHAR(MAX),
    ad_creative_id_str VARCHAR(MAX),
    media_id_str VARCHAR(MAX),
    apk VARCHAR(MAX),
    api VARCHAR(MAX),
    provider VARCHAR(MAX),
    show bigint(19),
    click bigint(19),
    cash_cost DOUBLE(53),
    cost DOUBLE(53),
    "convert" bigint(19),
    total bigint(19),
    total_male bigint(19),
    total_female bigint(19),
    total_good bigint(19),
    total_good_male bigint(19),
    total_good_female bigint(19),
    total_good_verified bigint(19),
    total_good_verified_male bigint(19),
    total_good_verified_female bigint(19),
    total_good_white bigint(19),
    total_good_verified_white bigint(19),
    total_good_young bigint(19),
    total_good_verified_young bigint(19),
    total_good_old bigint(19),
    total_good_verified_old bigint(19),
    total_good_ios bigint(19),
    total_good_verified_ios bigint(19),
    total_banned bigint(19),
    total_pending bigint(19),
    total_fake bigint(19),
    total_payed_amount DOUBLE(53),
    total_payed_amount_male DOUBLE(53),
    total_payed_amount_female DOUBLE(53),
    total_payed bigint(19),
    audit_durtions bigint(19),
    call_back_total_good_famle bigint(19),
    call_back_total_good_male bigint(19),
    call_back_total_good_total bigint(19),
    call_back_total_signup_total bigint(19),
    ecpm DOUBLE(53),
    active_plan bigint(19),
    bad_plan bigint(19),
    activation bigint(19),
    total_payed_amount_good DOUBLE(53),
    total_payed_amount_good_verified DOUBLE(53),
    total_payed_good bigint(19),
    total_payed_good_verified bigint(19),
    total_good_return_1d bigint(19),
    total_good_verified_return_1d bigint(19),
    hq bigint(19),
    greater_user bigint(19),
    greater_user_male bigint(19),
    greater_user_female bigint(19),
    third_line_city_user bigint(19),
    total_good_verified_twenty bigint(19),
    total_play bigint(19),
    valid_play bigint(19),
    play_duration_3s bigint(19),
    play_25_feed_break bigint(19),
    play_50_feed_break bigint(19),
    play_75_feed_break bigint(19),
    play_99_feed_break bigint(19),
    dy_like bigint(19),
    dy_comment bigint(19),
    dy_share bigint(19),
    dislike_cnt bigint(19),
    report_cnt bigint(19),
    url VARCHAR(MAX),
    ad_promotion_id_str VARCHAR(MAX),
    dt VARCHAR(MAX)
);

comment
on column dws.dws_ttx_market_media_reports_i_d.stat_date is ' 数据日期 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.stat_hour is ' 数据小时 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.channel is ' 渠道 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.agent is ' 代理 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.account is '账户';

comment
on column dws.dws_ttx_market_media_reports_i_d.ad_plan_id_str is '计划id';

comment
on column dws.dws_ttx_market_media_reports_i_d.ad_plan_name is '计划名称';

comment
on column dws.dws_ttx_market_media_reports_i_d.ad_creative_id_str is '创意id（投放平台id）';

comment
on column dws.dws_ttx_market_media_reports_i_d.media_id_str is '第三方兔子素材id';

comment
on column dws.dws_ttx_market_media_reports_i_d.show is '曝光';

comment
on column dws.dws_ttx_market_media_reports_i_d.click is '点击';

comment
on column dws.dws_ttx_market_media_reports_i_d.cash_cost is '现金花费';

comment
on column dws.dws_ttx_market_media_reports_i_d.cost is '账面花费';

comment
on column dws.dws_ttx_market_media_reports_i_d.total is '注册';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_male is '注册男性';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_female is '注册女性';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good is '注册且good总量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_male is '注册且good男量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_female is '注册且good女量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_verified is 'good且认证用户总量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_verified_male is 'good且认证用户男量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_verified_female is 'good且认证用户女量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_white is 'good白领总量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_verified_white is 'good且认证且白领总量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_young is 'good用户且年龄小于等于23岁';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_verified_young is 'good且认证用户且年龄小于等于23岁';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_old is 'good用户且年龄大于等于40岁';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_verified_old is 'good且认证用户且年龄大于等于40岁';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_ios is 'good iOS 总量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_verified_ios is 'good且认证 iOS 总量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_banned is 'banned用户量（非good用户）';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_pending is 'pending用户（待审核用户）';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_fake is 'fake用户量（非good用户）';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_payed_amount is '自助付费金额';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_payed_amount_male is '男性自助付费金额';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_payed_amount_female is '女性自助付费金额';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_payed is '总自助订单量';

comment
on column dws.dws_ttx_market_media_reports_i_d.audit_durtions is '审核时长';

comment
on column dws.dws_ttx_market_media_reports_i_d.call_back_total_good_famle is '回传good女用户';

comment
on column dws.dws_ttx_market_media_reports_i_d.call_back_total_good_male is '回传good男用户';

comment
on column dws.dws_ttx_market_media_reports_i_d.call_back_total_good_total is '回传good总用户量';

comment
on column dws.dws_ttx_market_media_reports_i_d.call_back_total_signup_total is '回传注册总量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_payed_amount_good is 'good用户自助付费总金额';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_payed_amount_good_verified is 'good且认证用户自助付费总金额';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_payed_good is 'good用户自助付费人数';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_payed_good_verified is 'good且认证用户自助付费人数';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_return_1d is 'good次日留存用户量';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_verified_return_1d is 'good且认证次日留存用户量';

comment
on column dws.dws_ttx_market_media_reports_i_d.hq is '优质用户：good且认证 且 23<=age<=40 且 学历非大专以下';

comment
on column dws.dws_ttx_market_media_reports_i_d.greater_user is '24+白领good认证人数';

comment
on column dws.dws_ttx_market_media_reports_i_d.greater_user_male is '优质用户（good且认证 且 23<=age<=40 且 学历非大专以下）男量';

comment
on column dws.dws_ttx_market_media_reports_i_d.greater_user_female is '优质用户（good且认证 且 23<=age<=40 且 学历非大专以下）女量';

comment
on column dws.dws_ttx_market_media_reports_i_d.third_line_city_user is 'good认证且城市等级三线及以下';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_good_verified_twenty is 'good且认证且年龄小于20';

comment
on column dws.dws_ttx_market_media_reports_i_d.total_play is ' 播放量  （大于0s）';

comment
on column dws.dws_ttx_market_media_reports_i_d.valid_play is ' 有效播放数 （大于等于10s）';

comment
on column dws.dws_ttx_market_media_reports_i_d.play_duration_3s is ' 3秒播放数 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.play_25_feed_break is ' 25%进度播放数 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.play_50_feed_break is ' 50%进度播放数 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.play_75_feed_break is ' 75%进度播放数 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.play_99_feed_break is ' 99%进度播放数 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.dy_like is ' 点赞量 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.dy_comment is ' 评论量 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.dy_share is ' 分享量 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.dislike_cnt is ' 不感兴趣量 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.report_cnt is ' 举报量 ';

comment
on column dws.dws_ttx_market_media_reports_i_d.url is '链接';

comment
on column dws.dws_ttx_market_media_reports_i_d.dt is 'date partition, format: yyyy-MM-dd';
```