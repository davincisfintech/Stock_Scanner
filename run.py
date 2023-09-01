if __name__ == '__main__':
    from scanner.controller import run, logger, scanner_class_dict, t

    ref_dict = {i: j for i, j in enumerate(scanner_class_dict)}
    try:
        print(ref_dict)
        filter_number = int(input(f'Enter number: '))
        logger.debug(f'Selected filter: {ref_dict[filter_number]}')
        run(ref_dict[filter_number])
    except (ValueError, KeyError) as e:
        logger.exception(e)
        logger.debug(f'Invalid input, it must be in {list(ref_dict.keys())}')
        t.sleep(3)
