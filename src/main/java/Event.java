/**
 * The Event that will carry the data.
 */
public class Event {

    private String message;

    public String getValue()
    {
        return this.message;
    }

    public void setValue(String message)
    {
        this.message = message;
    }

    public void clearValue()
    {
        this.message = null;
    }


}