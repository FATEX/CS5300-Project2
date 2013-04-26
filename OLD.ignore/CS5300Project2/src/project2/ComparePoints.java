package project2;

import java.util.Comparator;

/**
 * Used in conjunction with the Point class. This class compares points/nodes.
 */


//TODO: How is this different from this compareTo
public class ComparePoints implements Comparator<Point> {
	public int compare(Point p1, Point p2) {
		System.out.println(p1);
			if (p1.compareTo(p2) == 0) {
				return 0;
			} else {
				return (p1.getY() > p2.getY()) ? 1 : -1;
			}
	}
}
