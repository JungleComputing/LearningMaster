package ibis.learningmaster;

/**
 * Abstract superclass of all small messages. That is, messages that can be sent
 * connection-less because they are only a few bytes.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class SmallMessage extends Message {
    private static final long serialVersionUID = -8279209032498571638L;

}
