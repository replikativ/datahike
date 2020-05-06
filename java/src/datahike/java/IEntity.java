package datahike.java;

public interface IEntity {
    /**
     * Returns the value associated with keyword 'k' in this Entity.
     *
     * @param k a keyword
     * @return the value associated with keyword 'k' in this Entity.
     */
    public Object valAt(Object k);
}
