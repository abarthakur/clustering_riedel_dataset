import cc.refectorie.proj.relation.protobuf.DocumentProtos.Relation;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

class RelationReader {

  public static void main(String[] args) throws Exception {

  	String filePath = args[0]; 
  	String destPath= args[1];
  	FileInputStream input = new FileInputStream(filePath);
  	int i = 100000;
    FileOutputStream output;
    Relation rel;
    System.out.println("Begin processing.");
    while (input.available()>0){
	  	rel = Relation.parseDelimitedFrom(input);
  		System.out.println(rel.getSourceGuid() + " " +rel.getDestGuid() );
    	output = new FileOutputStream(destPath+i+".pb");
    	i++;
    	rel.writeTo(output);
    	output.close();
	}
	input.close();
	System.out.println("Done processing.");

  }
}
