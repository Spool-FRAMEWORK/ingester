package software.spool.ingester.fixture;

import software.spool.validator.api.Severity;
import software.spool.validator.api.Validate;
import software.spool.validator.api.Validator;

@Validate(value = Payment.class, sourceId = "payments", severity = Severity.WARNING)
public class NonZeroAmountWarningValidator implements Validator<Payment> {
    @Override
    public boolean validate(Payment payment) {
        return payment.amount() != 0;
    }

    @Override
    public String failMessage(Payment payment) {
        return "amount should not be zero";
    }
}
