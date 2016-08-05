/**
 * The MessageEvent that will carry the data.
 */
public class MessageEvent {

    private String message;

    public String getMessage()
    {
        return this.message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }

    public void clear()
    {
        this.message = null;
    }


}