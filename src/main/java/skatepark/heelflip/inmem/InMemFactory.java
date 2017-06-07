package skatepark.heelflip.inmem;

import skatepark.heelflip.IAggFactory;
import skatepark.heelflip.IFieldAgg;
import skatepark.heelflip.IGroupByAgg;

public class InMemFactory implements IAggFactory {

    @Override
    public IFieldAgg newFieldAgg(String fieldName) {
        return new InMemFieldAgg(fieldName);
    }

    @Override
    public IGroupByAgg newGroupByAgg(String fieldName, String groupBy) {
        return new InMemGroupByAgg(fieldName, groupBy);
    }
}
