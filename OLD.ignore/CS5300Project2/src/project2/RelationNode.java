package project2;

/**
 * This class establishes the parent child relationship with
 * a point and another node. This class is used only in LinkedNodes.java
 * to establish the source node to the destination node relationship.
 *
 */
public class RelationNode {
  Point point;
  RelationNode parent;

  public RelationNode(Point p) {
    point = p;
    parent = this;
  }
}