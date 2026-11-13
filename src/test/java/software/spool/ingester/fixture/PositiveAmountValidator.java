package software.spool.ingester.fixture;

import software.spool.validator.api.Severity;
import software.spool.validator.api.Validate;
import software.spool.validator.api.Validator;

@Validate(value = Payment.class, sourceId = "payments", severity = Severity.CRITICAL)
public class PositiveAmountValidator implements Validator<Payment> {
    @Override
    public boolean validate(Payment payment) {
        return payment.amount() > 0;
    }
}
