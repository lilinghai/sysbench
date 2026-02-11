#!/usr/bin/env sysbench

require("oltp_common")

function prepare_statements()
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
