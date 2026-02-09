#!/usr/bin/env sysbench

require("oltp_common")

function prepare_statements()
    if not sysbench.opt.skip_trx then
        prepare_begin()
        prepare_commit()
    end

    -- not conj
    if sysbench.opt.one_word_matchs > 0 then
        prepare_one_word_match()
    end
    if sysbench.opt.phrase_matchs > 0 then
        prepare_phrase_match()
    end
    if sysbench.opt.one_word_prefix_matchs > 0 then
        prepare_one_word_prefix_match()
    end

    -- same exprs conj
    if sysbench.opt.two_words_conj_matchs > 0 then
        prepare_two_words_conj_match()
    end
    if sysbench.opt.two_phrases_conj_matchs > 0 then
        prepare_two_phrases_conj_match()
    end
    if sysbench.opt.two_words_prefix_conj_matchs > 0 then
        prepare_two_words_prefix_conj_match()
    end

    -- diff exprs conj
    if sysbench.opt.phrase_word_conj_matchs > 0 then
        prepare_phrase_word_conj_match()
    end
    if sysbench.opt.phrase_prefix_conj_matchs > 0 then
        prepare_phrase_prefix_conj_match()
    end
    if sysbench.opt.word_prefix_conj_matchs > 0 then
        prepare_word_prefix_conj_match()
    end

    -- diff fields and exprs conj
    if sysbench.opt.two_fields_word_conj_matchs > 0 then
        prepare_two_fields_word_conj_match()
    end

    if sysbench.opt.two_fields_word_prefix_conj_matchs > 0 then
        prepare_two_fields_word_prefix_conj_match()
    end
end

function event()
    if not sysbench.opt.skip_trx then
        begin()
    end

    execute_one_word_match()
    execute_phrase_match()
    execute_one_word_prefix_match()

    execute_two_words_conj_match()
    execute_two_phrases_conj_match()
    execute_two_words_prefix_conj_match()

    execute_phrase_word_conj_match()
    execute_phrase_prefix_conj_match()
    execute_word_prefix_conj_match()

    execute_two_fields_word_conj_match()
    execute_two_fields_word_prefix_conj_match()

    if not sysbench.opt.skip_trx then
        commit()
    end

    check_reconnect()
end
