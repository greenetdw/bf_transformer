CREATE TABLE `member_info` (
  `member_id` varchar(32) NOT NULL DEFAULT '' COMMENT '��Աid����һ�����32λ����ĸ�����ַ���',
  `last_visit_date` date DEFAULT NULL COMMENT '����������',
  `created` date DEFAULT NULL,
  PRIMARY KEY (`member_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;