package ibis.learningmaster;

/**
 * A message sent by a peer to another peer, telling the destination peer that
 * the source peer won't talk to the destination peer anymore.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class CloseConnectionMessage extends SmallMessage {
    private static final long serialVersionUID = 1L;
}
