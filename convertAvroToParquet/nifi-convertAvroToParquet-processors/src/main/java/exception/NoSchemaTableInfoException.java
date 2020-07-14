package exception;

public class NoSchemaTableInfoException extends Exception{
  public String toString(){
    return "No schema or table info";
  }
}
