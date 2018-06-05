import com.thinhl.utils.ActivationFinderUtils;
import com.thinhl.model.PhoneActivationRecord;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class ActivationFinderTest {



    @Test
    public void multiplicationOfZeroIntegersShouldReturnZero() throws ParseException {

        List<PhoneActivationRecord> sample = new ArrayList<>(5);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(ActivationFinderUtils.DATE_PATTERN);

        sample.add(new PhoneActivationRecord("0987000001",
                simpleDateFormat.parse("2016-03-01"),
                simpleDateFormat.parse("2016-05-01")));



    }

}
