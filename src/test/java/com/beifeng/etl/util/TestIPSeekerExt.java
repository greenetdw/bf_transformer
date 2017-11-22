package com.beifeng.etl.util;

import java.util.List;

import com.beifeng.etl.util.IPSeekerExt.RegionInfo;

public class TestIPSeekerExt {
	public static void main(String[] args) {
		com.beifeng.etl.util.IPSeekerExt ipSeekerExt = new com.beifeng.etl.util.IPSeekerExt();
		RegionInfo info = ipSeekerExt.analyticIp("114.61.94.253");
		System.out.println(info);

//		List<String> ips = ipSeekerExt.getAllIp();
//		for (String ip : ips) {
//			System.out.println(ipSeekerExt.analyticIp(ip));
//		}
	}
}
