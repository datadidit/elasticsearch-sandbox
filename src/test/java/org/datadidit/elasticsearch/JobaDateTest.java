package org.datadidit.elasticsearch;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

public class JobaDateTest {
	@Test
	public void testDateParse() {
		String dateExample = "Dec 4, 1960";
		String simple = "12/08/1987";
		
		//DateTime dt = new DateTime();
		DateTimeFormatter fmt = DateTimeFormat.forPattern("MMM d, yyyy");
		//DateTimeFormatter fmt = DateTimeFormat.forPattern("MM/dd/yyyy");

		DateTime dt = fmt.parseDateTime(dateExample);
		System.out.println(dt);
	}
}
