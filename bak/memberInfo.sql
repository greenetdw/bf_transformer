CREATE TABLE `member_info` (
  `member_id` varchar(32) NOT NULL DEFAULT '' COMMENT '会员id，是一个最多32位的字母数字字符串',
  `last_visit_date` date DEFAULT NULL COMMENT '最后访问日期',
  `created` date DEFAULT NULL,
  PRIMARY KEY (`member_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;