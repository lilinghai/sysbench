#!/usr/bin/env sysbench

require("oltp_common")

function prepare_statements()
    if not sysbench.opt.skip_trx then
        prepare_begin()
        prepare_commit()
    end

    if sysbench.opt.one_word_matchs > 0 then
        prepare_one_word_match()
    end
    if sysbench.opt.two_words_and_matchs > 0 then
        prepare_two_words_and_match()
    end
    if sysbench.opt.two_words_or_matchs > 0 then
        prepare_two_words_or_match()
    end
    if sysbench.opt.two_fields_word_and_matchs > 0 then
        prepare_two_fields_word_and_match()
    end
    if sysbench.opt.two_fields_word_or_matchs > 0 then
        prepare_two_fields_word_or_match()
    end

    if sysbench.opt.one_word_prefix_matchs > 0 then
        prepare_one_word_prefix_match()
    end
    if sysbench.opt.two_words_and_prefix_matchs > 0 then
        prepare_two_words_and_prefix_match()
    end
    if sysbench.opt.two_words_or_prefix_matchs > 0 then
        prepare_two_words_or_prefix_match()
    end
    if sysbench.opt.two_fields_word_and_prefix_matchs > 0 then
        prepare_two_fields_word_and_prefix_match()
    end
    if sysbench.opt.two_fields_word_or_prefix_matchs > 0 then
        prepare_two_fields_word_or_prefix_match()
    end

    if sysbench.opt.mix_prefix_and_word_matchs > 0 then
        prepare_mix_prefix_and_word_match()
        prepare_mix_prefix_and_word_match2()
    end
    if sysbench.opt.mix_prefix_or_word_matchs > 0 then
        prepare_mix_prefix_or_word_match()
        prepare_mix_prefix_or_word_match2()
    end
end

function event()
    if not sysbench.opt.skip_trx then
        begin()
    end

    execute_one_word_match()
    execute_two_words_and_match()
    execute_two_words_or_match()
    execute_two_fields_word_and_match()
    execute_two_fields_word_or_match()

    execute_one_word_prefix_match()
    execute_two_words_and_prefix_match()
    execute_two_words_or_prefix_match()
    execute_two_fields_word_and_prefix_match()
    execute_two_fields_word_or_prefix_match()

    execute_mix_prefix_and_word_match()
    execute_mix_prefix_or_word_match()
    if not sysbench.opt.skip_trx then
        commit()
    end

    check_reconnect()
end
