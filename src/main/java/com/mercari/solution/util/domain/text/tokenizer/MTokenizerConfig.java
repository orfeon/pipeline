package com.mercari.solution.util.domain.text.tokenizer;

import java.io.Serializable;

public class MTokenizerConfig implements Serializable {

    public Integer maxLength;
    public Integer stride;
    public Boolean padding;
    public Boolean addSpecialTokens;
    public Boolean truncation;
    public Boolean withOverflowingTokens;

}
