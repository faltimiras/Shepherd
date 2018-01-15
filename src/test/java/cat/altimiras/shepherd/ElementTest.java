package cat.altimiras.shepherd;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ElementTest {

	private Element<Integer> original = new Element("key", Arrays.asList(1, 2, 3, 4, 5));

	@Test
	public void positive() throws Exception {

		Element element = new Element(original, 2, false);
		assertEquals(Arrays.asList(1, 2), element.getValues());

	}

	@Test
	public void positiveDiscard() throws Exception {

		Element element = new Element(original, 2, true);
		assertEquals(Arrays.asList(4, 5), element.getValues());
	}

	@Test
	public void negative() throws Exception {

		Element element = new Element(original, -1, false);
		assertEquals(Arrays.asList(5), element.getValues());
	}

	@Test
	public void negativeDiscard() throws Exception {

		Element element = new Element(original, -2, true);
		assertEquals(Arrays.asList(1, 2, 3), element.getValues());

	}

}
