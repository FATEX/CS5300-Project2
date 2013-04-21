package project2;

/**
 * This interface imposes a total ordering 
 * on the objects of each class that implements it. 
 * This ordering is referred to as the class's natural ordering, and the class's compareTo method is 
 * referred to as its natural comparison method.*/

public class Point implements Comparable<Point>{
  public double x;
  public double y;

  public Point(double xCoord, double yCoord) {
    x = xCoord;
    y = yCoord;
  }

  public Point(String input) {
    String[] parts = input.split(":");
    x = Double.valueOf((parts[0]));
    y = Double.valueOf((parts[1]));
  }

//Setters & Getters-----------------
  public void setX(double x) {
    this.x = x;
  }

  public double getX() {
    return x;
  }

  public void setY(double y) {
    this.y = y;
  }

  public double getY() {
    return y;
  }
//---------------------------------
  
//Comparison function to check whether the point is the same point and if not, what the difference is.
  public int compareTo(Point p){
    double diff = this.x-p.getX();
    if (diff == 0.0){
      diff = this.y - p.getY();
      if (diff == 0.0){
        return 0;
      }
      else{
    	  //Returns 1 for TRUE and -1 for FALSE
    	  return (diff > 0) ? 1 : -1;
      }
    }
    else{
      return (diff > 0) ? 1 : -1;
    }
  }
  
  public boolean equals(Point p){
    if (this.compareTo(p) == 0){
      return true;
    }
    else{
      return false;
    }
  }

  public String toString() {
    return x + ":" + y;
  }

//TODO: Talk about why we need this?
  public int hashCode(){
    return this.toString().hashCode();
  }
}