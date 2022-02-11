<% String errMesg=">>>>>>> " +odiRef.getOption("ERROR_MESSAGE") + " <<<<<<<<"; %>
<$

// New Class being defined for raising custom exceptions
// New Class TableException-

public class CustomException extends Exception
{

                  String errmesg;
                  public CustomException()
                  {
                    super();             // call superclass constructor
                    errmesg = "CustomException:Unknown Error Occured";
                  }

                // Constructor with the error message
                  public CustomException(String err)
                  {
                    super(err);     // call super class constructor
                    errmesg = err;  // save message
                  }

                // public method, callable by exception catcher. It returns the error message.
                  public String getError()
                  {
                    return errmesg;
                  }

}
//end of class  CustomException

String mesg="<%=errMesg%>";
CustomException custErr=new CustomException(mesg);
throw custErr;

$>
