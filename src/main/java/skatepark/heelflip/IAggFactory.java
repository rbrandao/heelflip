package skatepark.heelflip;

public interface IAggFactory {

    IFieldAgg newFieldAgg(String fieldName);

    IGroupByAgg newGroupByAgg(String fieldName, String groupBy);
}
