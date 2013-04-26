package project2;

public class Edge {
  Point point1;
  Point point2;
  
  public Edge(double x1, double y1, double x2, double y2){
	  point1 = new Point(x1,y1);
	  point2 = new Point(x2,y2);
  }
  
  public Edge(Point p1, Point p2){
	  point1 = p1;
	  point2 = p2;
  }
  
  public String toString(){
    return point1.toString() + "-" + point2.toString();
  }
}