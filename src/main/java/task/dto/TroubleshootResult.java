package task.dto;

import lombok.Data;

import java.util.List;

/**
 * @author dwj
 * @date 2020/11/23 10:15
 */
@Data
public class TroubleshootResult {

    private Long troubleshootId;

    private Long id;

    private String name;

    private Long time;

    private Timeseries timeseries;

    @Data
    public static class Timeseries {
        private List<String> properties;

        private List<Object[]> values;
    }
}
