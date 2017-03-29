.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
       movq $86004, %rcx
       s28:
         mulss  value1, %xmm0
         mulss  %xmm1, %xmm0
         mulss  %xmm1, %xmm0
         mulss  %xmm1, %xmm0
         mulss  %xmm1, %xmm0
         mulss  %xmm1, %xmm0
         mulss  %xmm1, %xmm0
	 loop s28
