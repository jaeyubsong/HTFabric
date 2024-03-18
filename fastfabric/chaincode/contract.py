def execute(rwset, args):
    if len(args) == 4:
        account1 = args[1]
        account2 = args[2]
        transfer_amount = int(args[3])
        acc1_value = int(rwset[0][account1])
        acc2_value = int(rwset[0][account2])
        if acc1_value - transfer_amount >= 0:
            rwset[1][account1] = str(acc1_value - transfer_amount)
            rwset[1][account2] = str(acc2_value + transfer_amount)
    elif len(args) == 10:
        account1 = args[1]
        account2 = args[2]
        account3 = args[3]
        account4 = args[4]
        account5 = args[5]
        account6 = args[6]
        account7 = args[7]
        account8 = args[8]
        transfer_amount = int(args[9])
        acc1_value = int(rwset[0][account1])
        acc2_value = int(rwset[0][account2])
        acc3_value = int(rwset[0][account3])
        acc4_value = int(rwset[0][account4])
        acc5_value = int(rwset[0][account5])
        acc6_value = int(rwset[0][account6])
        acc7_value = int(rwset[0][account7])
        acc8_value = int(rwset[0][account8])
        if acc1_value - transfer_amount >= 0:
            rwset[1][account1] = str(acc1_value - transfer_amount)
            rwset[1][account2] = str(acc2_value + transfer_amount)
            rwset[1][account3] = str(acc3_value + transfer_amount)
            rwset[1][account4] = str(acc4_value + transfer_amount)
            rwset[1][account5] = str(acc5_value + transfer_amount)
            rwset[1][account6] = str(acc6_value + transfer_amount)
            rwset[1][account7] = str(acc7_value + transfer_amount)
            rwset[1][account8] = str(acc8_value + transfer_amount)
    else:
        account1 = args[0]
        account2 = args[1]
        transfer_amount = int(args[2])

        acc1_value = int(rwset[0][account1])
        acc2_value = int(rwset[0][account2])
        if acc1_value - transfer_amount >= 0:
            rwset[1][account1] = str(acc1_value - transfer_amount)
            rwset[1][account2] = str(acc2_value + transfer_amount)

    rwset[0]['benchmark'] = ''
    return rwset
