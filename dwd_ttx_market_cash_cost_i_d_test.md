```
-- auto-generated definition
CREATE TABLE dwd.dwd_ttx_market_cash_cost_i_d
(
    stat_date VARCHAR(MAX
) ,
    stat_hour bigint(19),
    channel VARCHAR(MAX),
    agent VARCHAR(MAX),
    account VARCHAR(MAX),
    ad_plan_id_str VARCHAR(MAX),
    ad_plan_name VARCHAR(MAX),
    apk VARCHAR(MAX),
    api VARCHAR(MAX),
    provider VARCHAR(MAX),
    show bigint(19),
    click bigint(19),
    cash_cost DOUBLE(53),
    cost DOUBLE(53),
    "convert" bigint(19),
    download_start bigint(19),
    download_finish bigint(19),
    install bigint(19),
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
    call_back_total_good_verified_total bigint(19),
    total_payed_amount_after_tax DOUBLE(53),
    total_payed_amount_after_tax_male DOUBLE(53),
    total_payed_amount_after_tax_female DOUBLE(53),
    total_payed_amount_after_tax_good DOUBLE(53),
    total_payed_amount_after_tax_good_verified DOUBLE(53),
    account_name VARCHAR(MAX),
    account_note VARCHAR(MAX),
    total_good_verified_22_40 bigint(19),
    dt VARCHAR(MAX)
);

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.stat_date is ' 数据日期 ';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.stat_hour is ' 数据小时 ';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.channel is ' 渠道 ';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.agent is ' 代理 ';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.account is '账户';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.show is '曝光';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.click is '点击';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.cash_cost is '现金花费';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.cost is '账面花费';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total is '注册';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_male is '注册男';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_female is '注册女';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good is '注册且good总量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_male is '注册且good男量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_female is '注册且good女量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified is 'good且认证用户总量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified_male is 'good且认证用户男量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified_female is 'good且认证用户女量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_white is 'good白领总量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified_white is 'good且认证且白领总量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_young is 'good用户且年龄小于等于23岁';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified_young is 'good且认证用户且年龄小于等于23岁';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_old is 'good用户且年龄大于等于40岁';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified_old is 'good且认证用户且年龄大于等于40岁';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_ios is 'good iOS 总量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified_ios is 'good且认证 iOS 总量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_banned is 'banned用户量（非good用户）';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_pending is 'pending用户（待审核用户）';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_fake is 'fake用户量（非good用户）';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_payed_amount is '自助付费金额（牵手市场关注花费皆为自助花费，后端过滤 红娘商品类型：  0,5,6,7,13）';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_payed_amount_male is '男性自助付费金额';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_payed_amount_female is '女性自助付费金额';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_payed is '自助订单数';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.audit_durtions is '审核时长';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.call_back_total_good_famle is '回传good女用户';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.call_back_total_good_male is '回传good男用户';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.call_back_total_good_total is '回传good总用户量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.call_back_total_signup_total is '回传注册总量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.ecpm is 'none';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_payed_amount_good is 'good用户自助付费金额';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_payed_amount_good_verified is 'good且认证用户自助付费金额';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_payed_good is 'good自助商品付费人数';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_payed_good_verified is 'good且认证自助商品付费人数';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_return_1d is 'good次日留存用户量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified_return_1d is 'good且认证次日留存用户量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.hq is '优质用户：good且认证 且 23<=age<=40 且 学历非大专以下';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.greater_user is '24+白领good认证人数';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.greater_user_male is '优质用户（good且认证 且 23<=age<=40 且 学历非大专以下）男量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.greater_user_female is '优质用户（good且认证 且 23<=age<=40 且 学历非大专以下）女量';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.third_line_city_user is 'good认证且城市等级三线及以下';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified_twenty is 'good且认证且年龄小于20';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.call_back_total_good_verified_total is '认证回传数';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.account_name is '账户名称';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.account_note is '账户备注';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.total_good_verified_22_40 is 'good且认证且年龄大于22且小于40岁';

comment
on column dwd.dwd_ttx_market_cash_cost_i_d.dt is 'date partition, format: yyyy-MM-dd';
```