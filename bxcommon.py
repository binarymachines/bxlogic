#!/usr/bin/env python

import re
from snap import common


class ListOutputResponder(object):
    def __init__(self, generator_command_spec, command_parse_function, **kwargs):
        self.cmd_spec = generator_command_spec
        self.command_parse_func = command_parse_function
        self.singular_item_noun = kwargs.get('single_item_noun', 'object')
        self.plural_item_noun = kwargs.get('plural_item_noun', 'objects')
        self.pos_integer_rx = re.compile(r'^[0-9]+$')
        self.neg_integer_rx = re.compile(r'^-[0-9]+$')
        self.range_rx = re.compile(r'^[0-9]+\-[0-9]+$')

    def extension_is_positive_num(self, ext_string):
        if self.pos_integer_rx.match(ext_string):
            return True
        return False

    def extension_is_negative_num(self, ext_string):
        if self.neg_integer_rx.match(ext_string):
            return True
        return False

    def extension_is_range(self, ext_string):
        if self.range_rx.match(ext_string):
            return True
        return False

    def detect_filter_expression(self, cmd_object):
        '''
        <cmd><filter_char><exp>.<selector>

        or

        <cmd><filter_char><exp>
        '''
        CHARS_TO_ESCAPE = ['?', '+']

        command_string = cmd_object.cmd_string
        fchar = cmd_object.cmdspec.filterchar
        spec = cmd_object.cmdspec.specifier

        if fchar in CHARS_TO_ESCAPE:
            fchar_seq = "\\" + fchar
        else:
            fchar_seq = fchar

        filter_expr_at_end_rx = re.compile(r'{fchar}[a-zA-z0-9\-]+$'.format(fchar=fchar_seq))
        filter_expr_with_ext_rx = re.compile(r'{fchar}[a-zA-z0-9\-]+.'.format(fchar=fchar_seq))

        #
        # the filter expression is the part of the command string between the filter character and:
        # -- end of string if there is no specifier char; or
        # -- the specifier char.
        #
        #

        match = filter_expr_at_end_rx.search(command_string)
        if match:
            start_index = match.span()[0]
            extent = match.span()[1]
            return command_string[start_index:extent].lstrip(fchar)

        match = filter_expr_with_ext_rx.search(command_string)
        if match:
            start_index = match.span()[0]
            extent = match.span()[1]
            return command_string[start_index:extent].lstrip(fchar).rstrip(spec)

        return '*'  # if no match, filter expression is wildcard

    def generate(self, **kwargs):

        kwreader = common.KeywordArgReader('command_object',
                                           'record_list',
                                           'render_callback',
                                           'filter_callback',
                                           'dialog_context',
                                           'dialog_engine',
                                           'service_registry')
        kwreader.read(**kwargs)

        cmd_object = kwargs['command_object']
        rec_list = kwargs['record_list']
        render_callback = kwargs['render_callback']
        filter_function = kwargs['filter_callback']
        dlg_context = kwargs['dialog_context']
        dlg_engine = kwargs['dialog_engine']
        service_registry = kwargs['service_registry']

        filter_expression = self.detect_filter_expression(cmd_object)
        if filter_expression == '*':
            items = rec_list
        else:
            items = [item for item in rec_list if filter_function(item, filter_expression)]

        # we will either receive a plain command string,
        # or a command string followed immediately by specifier
        # and an extesion (for lists, usually a numerical selector)
        #
        tokens = cmd_object.cmd_string.split(cmd_object.cmdspec.specifier)
        if len(tokens) == 1:
            lines = []
            index = 1

            # if no specifier is present in the command string,
            # return the entire list (with indices)
            # TODO: segment output for very long lists

            for item in items:
                lines.append(render_callback(index, item))
                index += 1

            return '\n\n'.join(lines)

        else:
            ext = tokens[1]
            # the "extension" is the part of the command string immediately
            # following the specifier character.
            #
            # if we receive <cmd><specifier>N where N is an integer,
            # return the Nth item in the list
            #
            if self.extension_is_positive_num(ext):
                list_index = int(ext)

                if list_index > len(items):
                    return ("You requested open job # %d, but there are only %d %s in this list."
                            % (list_index, len(items), self.plural_item_noun))

                if list_index == 0:
                    return "You may not request the 0th element of a list. (Nice try, C programmers.)"

                list_element = items[list_index-1]

                # if the user is extracting a single list element (by using an integer extension), we do 
                # one of two things. If there were no command modifiers specified, we simply return the element:
                #
                if not len(cmd_object.modifiers):
                    return render_callback(0, list_element)
                else:
                    # ...but if there were modifiers, then we construct a new command by chaining 
                    # the output of this command with the modifiers passed to us.
                    #
                    command_tokens = [list_element]
                    command_tokens.extend(cmd_object.modifiers)

                    # parse function reads urlquoted strings, so sub + for spaces
                    command_string = '+'.join(command_tokens)
                    chained_command = self.command_parse_func(command_string)

                    print('command: ' + str(chained_command))                                       
                    return dlg_engine.reply_command(chained_command, dlg_context, service_registry)

            elif self.extension_is_negative_num(ext):
                neg_index = int(ext)
                if neg_index == 0:
                    return '-0 is not a valid negative index. Use -1 to specify the last %s in the list.' % self.singular_item_noun

                zero_list_index = len(items) + neg_index

                if zero_list_index < 0:
                    return ('You specified a negative list offset (%d), but there are only %d %s in the list.' 
                            % (neg_index, len(items), self.plural_item_noun))

                list_element = items[zero_list_index]

                if not len(cmd_object.modifiers):
                    return list_element
                else:
                    # ...but if there were modifiers, then we construct a new command 
                    # by chaining the output of this command with the modifier array.
                    command_tokens = [list_element]
                    command_tokens.extend(cmd_object.modifiers)

                    # TODO: instead of splitting on this char, urldecode the damn thing from the beginning
                    command_string = '+'.join(command_tokens)
                    chained_command = self.command_parse_func(command_string)

                    print('command: ' + str(chained_command))                                       
                    return dlg_engine.reply_command(chained_command, dlg_context, service_registry)

            elif self.extension_is_range(ext):
                # if we receive <cmd><specifier>N-M where N and M are both integers, return the Nth through the Mth items
                tokens = ext.split('-')
                if len(tokens) != 2:
                    return 'The range extension for this command must be formatted as A-B where A and B are integers.'

                min_index = int(tokens[0])
                max_index = int(tokens[1])
                
                if min_index > max_index:
                    return 'The first number in your range specification A-B must be less than or equal to the second number.'

                if max_index > len(items):
                    return "There are only %d %s open." % (len(items), self.plural_item_noun)

                if min_index == 0:
                    return "You may not request the 0th element of a list. (This stack was written in Python, but the UI is in English.)"

                if not len(cmd_object.modifiers):
                    lines = []
                    for index in range(min_index, max_index+1):
                        lines.append(render_callback(index, items[index-1]))

                    return '\n\n'.join(lines)
                else:
                    # ...but if there were modifiers, then for each element in the filtered list...
                    #
                    lines = []
                    for index in range(min_index, max_index+1):
                        # ...we construct a new command by chaining the current element
                        # with the modifier array

                        command_tokens = [items[index-1]]
                        command_tokens.extend(cmd_object.modifiers)

                        # sub + for spaces
                        command_string = '+'.join(command_tokens)
                        chained_command = self.command_parse_func(command_string)

                        print('command: ' + str(chained_command))                                       
                        lines.append(dlg_engine.reply_command(chained_command, dlg_context, service_registry))

                    return '\n\n'.join(lines)
