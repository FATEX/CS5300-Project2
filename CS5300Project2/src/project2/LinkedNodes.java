package project2;

import java.util.HashMap;

/**
 * This class LinkedNodes.java is used to establish the links between any given point and other nodes. 
 * TODO: Finish description
 *
 */

public class LinkedNodes {
  private HashMap<String, RelationNode> table; //stores the point and its union node
  
  public LinkedNodes(Iterable<Point> points) {
    table = new HashMap<String, RelationNode>();
    for (Point p : points) {
      table.put(p.toString(), new RelationNode(p));
    }
  }
  
  public RelationNode find(Point p) {
	    RelationNode pNode = table.get(p.toString());
	    while (pNode.parent != pNode) {
	      pNode = pNode.parent;
	    }
	    return pNode;
  }
  
  public void union(Point p1, Point p2) {
    RelationNode p1Node = find(p1);
    RelationNode p2Node = find(p2);
    int comparison = p1Node.point.compareTo(p2Node.point);
    if (comparison < 0) {
      p2Node.parent = p1Node;
    } else if (comparison > 0) {
      p1Node.parent = p2Node;
    }
  }
}